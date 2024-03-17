import streamlit

import streamlit.web.cli as stcli
import os, sys


def resolve_path(path):
    resolved_path = os.path.abspath(os.path.join(os.getcwd(), path))
    return resolved_path


if __name__ == "__main__":
    path = resolve_path("front.py")
    path = "C:\\Users\\orlan\\Documents\\Documents\\omni-raids\\app\\front.py"
    sys.argv = [
        "streamlit",
        "run",
        path,
        "--global.developmentMode=false",
    ]
    sys.exit(stcli.main())