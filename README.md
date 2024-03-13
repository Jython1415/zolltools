# zolltools

The `zolltools` package is a collection of tools and scripts for work with NEMSIS data for the purpose of analyzing and reporting on EMS data. The package has "zoll" in it because it was originally developed as part of a research project funded by a grant from the ZOLL Foundation.

## Logging

Logger names for each of the messages follow standard convention. Each module has its own logger, and they can be accessed with `logging.get_logger()`. The name of each module's logger also follows standard convention. For example, the name of the logger for the `sasconvert` module would be `"zolltools.db.sasconvert"`. Included below is a short example of how one can access the logs for the `sasconvert` module. See [Python's documentation of the logging module](https://bit.ly/469APRI) for more information on how to format and handle the logs.

```Python
import logging
import zolltools

sasconvert_logger = logging.getLogger("zolltools.db.sasconvert")
sasconvert_logger.setLevel(logging.DEBUG)
sasconvert_logger.addHandler(logging.FileHandler("sasconvert.log"))
```

To add a handler or set the level of all loggers in the package at once (and for other similar convenience functions), see the `zolltools.logging` module. See the example usage below.

```Python
import logging
from zolltools import logging as zlg

zoll_handler = logging.FileHandler("main.log")
zoll_level = logging.WARNING
zlg.set_level(zoll_level)
zlg.add_handler(zoll_handler)
```

Log messages from functions in the `zolltools` package will include a prefix in the following format:

```text
<class name if in class>.<function name>: <message>
```

## Development

### Installation

To initialize the project for development, clone the repository and run the initialization script.

```bash
git clone https://github.com/Jython1415/zolltools.git # Clone the repo
cd zolltools         # Navigate to the project root
./scripts/install.sh # Initialize the project
```

### Testing

To run tests, use the following command after activating the virtual environment:

```bash
python -m pytest
```

### Building

To build and upload the project to test PyPI, use the following command:

```bash
bash scripts/new-build.sh
```
