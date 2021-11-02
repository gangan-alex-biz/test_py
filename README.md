Python script that does the following:
Download any csv from web (cols > 5, rows > 10)
Cache option, if True use cached version instead of downloading
Settings in yaml file (use_cache, cache_folder, output_format, etc)
Logger that logs every step
Read the file's first 5 cols and 10 rows
Output the result to json or csv depending on settings (make both options)