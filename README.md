# zolltools

## Logging

Logger names for each of the messages follow standard convention. Each module
has its own logger, and they can be accessed with `logging.get_logger()`. The
name of each module's logger also follows standard convention. For example, the
name of the logger for the `sasconvert` module would be
`"zolltools.db.sasconvert"`. Included below is a short example of how one can
access the logs from the `sasconvert` module. See
[Python's documentation of the logging module](https://bit.ly/469APRI)
for more information on how to format and handle the logs.

```Python
import logging

sasconvert_logger = logging.getLogger("zolltools.db.sasconvert")
sasconvert_logger.setLevel(logging.DEBUG)
sasconvert_logger.addHandler(logging.FileHandler("sasconvert.log"))
```
