#### FILE SOURCE - SOURCE INPUT
- In the case of files being received every few seconds, after a point of time the 
number of files in the directory will be keep increasing.
- If we have a lot of files in folder then it will slow down the read of the subsequent files.
- It is recommended to clean up the older files when possible.
- Options to achieve the archive of the files are
  1. cleanSource & sourceArchiveDir are often used together
  2. .option("cleanSource", "delete") - DELETE THE FILES after they are consumed.
  3. .option("cleanSource", "archive") & .option("sourceArchiveDir", "Name-of-archive-dir")
  4. In the above option, the files are archived immediately but it slows down the job run, if there is need for quick processing then this need to be avoided.
  5. In such cases you can have your own batch job scheduled which will take care of this cleanup.
