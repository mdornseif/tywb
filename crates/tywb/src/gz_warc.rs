//! Per-member gzip reader for `.warc.gz` files.
//!
//! A standard `.warc.gz` archive stores each WARC record as its own gzip member
//! (concatenated together in one file). To replay an archived page we need the
//! **compressed** byte offset of the member — information a `MultiGzDecoder`
//! cannot provide because it joins all members into one seamless stream.
//!
//! [`GzSplitter`] reads one member at a time and reports the compressed offset
//! of each, enabling precise S3 Range-GET replay.

use std::io::{self, Read};
use flate2::{Decompress, FlushDecompress, Status};

/// Reads a concatenated gzip stream one member at a time.
///
/// Returns `(compressed_start, decompressed_bytes)` for each member via
/// [`next_member`][GzSplitter::next_member].
///
/// The `compressed_start` value is the byte offset of the gzip member in the
/// *compressed* source stream.  Store this in the CDX `c_offset` column so
/// the replay handler can issue a minimal S3 Range GET.
pub struct GzSplitter<R: Read> {
    source:    R,
    buf:       Vec<u8>, // compressed bytes buffered from source
    buf_start: usize,   // index of first unconsumed byte in buf
    raw_pos:   u64,     // total bytes ever read from source into buf
}

impl<R: Read> GzSplitter<R> {
    /// Wrap `source` in a splitter.  `source` should supply raw compressed bytes
    /// (e.g. a `ChannelReader` streaming an S3 object body).
    pub fn new(source: R) -> Self {
        Self {
            source,
            buf: Vec::with_capacity(128 * 1024),
            buf_start: 0,
            raw_pos: 0,
        }
    }

    /// Logical compressed position: bytes read from source minus buffered remainder.
    pub fn pos(&self) -> u64 {
        self.raw_pos - (self.buf.len() - self.buf_start) as u64
    }

    fn available(&self) -> usize {
        self.buf.len() - self.buf_start
    }

    /// Compact: move unconsumed bytes to the front of the buffer.
    fn compact(&mut self) {
        if self.buf_start > 0 {
            self.buf.copy_within(self.buf_start.., 0);
            self.buf.truncate(self.buf.len() - self.buf_start);
            self.buf_start = 0;
        }
    }

    /// Attempt to fill the buffer so at least `n` bytes are available.
    /// Returns `false` if EOF was reached before `n` bytes could be buffered.
    fn ensure(&mut self, n: usize) -> io::Result<bool> {
        while self.available() < n {
            self.compact();
            let old = self.buf.len();
            let extra = n.max(65536);
            self.buf.resize(old + extra, 0);
            let k = self.source.read(&mut self.buf[old..])?;
            self.buf.truncate(old + k);
            self.raw_pos += k as u64;
            if k == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Like `ensure` but returns `UnexpectedEof` on failure.
    fn need(&mut self, n: usize) -> io::Result<()> {
        if !self.ensure(n)? {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("unexpected EOF: need {n} bytes, have {}", self.available()),
            ))
        } else {
            Ok(())
        }
    }

    /// Consume `n` bytes from the buffer, returning a copy.
    fn take_vec(&mut self, n: usize) -> Vec<u8> {
        let v = self.buf[self.buf_start..self.buf_start + n].to_vec();
        self.buf_start += n;
        v
    }

    /// Skip `n` bytes from the buffer (no copy).
    fn skip(&mut self, n: usize) {
        self.buf_start += n;
    }

    /// Skip a NUL-terminated byte string (consumes the NUL too).
    fn skip_nul_string(&mut self) -> io::Result<()> {
        loop {
            self.need(1)?;
            let b = self.buf[self.buf_start];
            self.buf_start += 1;
            if b == 0 {
                return Ok(());
            }
        }
    }

    /// Read the next gzip member from the source.
    ///
    /// Returns `Ok(Some((compressed_start, decompressed)))` on success,
    /// `Ok(None)` at clean EOF between members, or an error for a corrupt stream.
    pub fn next_member(&mut self) -> io::Result<Option<(u64, Vec<u8>)>> {
        // Allow clean EOF between members.
        if !self.ensure(1)? {
            return Ok(None);
        }

        let member_start = self.pos();

        // ── Gzip fixed header (10 bytes) ──────────────────────────────────────
        self.need(10)?;
        let hdr = self.take_vec(10);

        if hdr[0] != 0x1f || hdr[1] != 0x8b {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bad gzip magic {:#04x} {:#04x} at compressed offset {member_start}",
                    hdr[0], hdr[1]
                ),
            ));
        }
        if hdr[2] != 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported gzip CM={} at {member_start}", hdr[2]),
            ));
        }
        let flg = hdr[3];

        // ── Optional header fields ────────────────────────────────────────────
        // FEXTRA
        if flg & 0x04 != 0 {
            self.need(2)?;
            let xlen = u16::from_le_bytes([self.buf[self.buf_start], self.buf[self.buf_start + 1]])
                as usize;
            self.skip(2);
            self.need(xlen)?;
            self.skip(xlen);
        }
        // FNAME
        if flg & 0x08 != 0 {
            self.skip_nul_string()?;
        }
        // FCOMMENT
        if flg & 0x10 != 0 {
            self.skip_nul_string()?;
        }
        // FHCRC
        if flg & 0x02 != 0 {
            self.need(2)?;
            self.skip(2);
        }

        // ── DEFLATE data ──────────────────────────────────────────────────────
        let mut decompress = Decompress::new(false);
        let mut out: Vec<u8> = Vec::with_capacity(128 * 1024);

        loop {
            // Ensure there is compressed input available.
            if self.available() == 0 {
                self.compact();
                let old = self.buf.len();
                self.buf.resize(old + 65536, 0);
                let k = self.source.read(&mut self.buf[old..])?;
                self.buf.truncate(old + k);
                self.raw_pos += k as u64;
                if k == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "EOF inside deflate stream",
                    ));
                }
            }

            // Ensure the output buffer has room before calling decompress_vec.
            out.reserve(65536);

            let before_in = decompress.total_in();
            let status = decompress
                .decompress_vec(
                    &self.buf[self.buf_start..],
                    &mut out,
                    FlushDecompress::None,
                )
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let consumed = (decompress.total_in() - before_in) as usize;
            self.buf_start += consumed;

            if status == Status::StreamEnd {
                break;
            }
        }

        // ── Trailer: CRC32 (4 bytes) + ISIZE (4 bytes) ───────────────────────
        self.need(8)?;
        self.skip(8);

        Ok(Some((member_start, out)))
    }
}
