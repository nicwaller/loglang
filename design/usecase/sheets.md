# Google Sheets

Periodically scrape a Google Sheet and generate an event for every row. Every column becomes a field in the event.

If the row has a timestamp, it's possible to generate events only for **new** rows.

Or generate alerts for upcoming future events (like expiring domains) by comparing timestamp against current date. 
