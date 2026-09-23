"""Runtime setup used only by the Week 2 Spark entry points."""
# Only added as it we got right output but had lots of JAR related issue
import os
from pathlib import Path


def configure_spark_temp_dir() -> Path:
    """Use a user-writable Spark temporary directory on Windows.

    Spark copies resolved dependency JARs into its temporary directory.  The
    default on this machine is ``C:\\Windows\\Temp``, where cleanup can fail
    when Windows briefly keeps a JAR handle open.  These process-local values
    are set before the JVM starts, leaving the user's global environment
    untouched.
    """
    base = Path(os.environ.get("LOCALAPPDATA", Path.cwd())) / "Temp"
    temp_dir = base / "urban-data-platform-spark"
    temp_dir.mkdir(parents=True, exist_ok=True)

    temp_dir_text = str(temp_dir)
    os.environ["TEMP"] = temp_dir_text
    os.environ["TMP"] = temp_dir_text
    os.environ["SPARK_LOCAL_DIRS"] = temp_dir_text

    return temp_dir
