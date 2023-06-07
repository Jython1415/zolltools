"""Module that implements removeprefix and removesuffix for Python 3.8"""

def removeprefix(string, prefix) -> str:
    """Removes prefix from string"""
    if len(prefix) != 0 and string.startswith(prefix):
        return string[len(prefix):]
    return string

def removesuffix(string, suffix) -> str:
    """Removes suffix from string"""
    if len(suffix) != 0 and string.endswith(suffix):
        return string[:-len(suffix)]
    return string

def main():
    """Method that is run if this file is executed"""

if __name__ == "__main__":
    main()
