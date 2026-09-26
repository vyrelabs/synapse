# Filter

The `filter` package provides Domain/URL filtering, /w support for both local and 
distributed impls. All needed is to replace the underlying `SeenSet`, which tracks 
the processed/visited Domains/URLS, depending on the caller side.
