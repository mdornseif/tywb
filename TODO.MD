* for each WARC file you process also store the bucket name you fond it in
  and also store the warcinfo record (WARK header) in sqlite
* UI should allow browsing by domainname. eg list of TLDs -> Licht of
  doamains -> list of second level domains
* if they do not exist write out cdx files in paralell to the WARC files
  into the bucket.
- Wayback toolbar - inject a sticky archive bar into replayed HTML pages (date, URL, link to other captures)          
- indexed=0 fulltext issue ??? records in CDX but missing from search                                                   
- warcinfo records in search results ??? urn:X-wpull:log appearing as search hits (filter non-HTTP records from the text  index)        