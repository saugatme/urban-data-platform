# Setup Log — How This Environment Was Built

Full history of getting Spark 3.5 + Delta Lake 3.2 working on Windows 11.
If setup breaks for you, find the matching symptom here.

## Environment

| Component | Version |
| --- | --- |
| OS | Windows 11 |
| Java | Eclipse Temurin 17 |
| Python | 3.12 (via uv venv) |
| PySpark | 3.5.x |
| Delta Lake | 3.2.x |
| Hadoop helpers | winutils.exe + hadoop.dll (hadoop-3.0.0) in `C:\hadoop\bin` |

## The Journey (chronological)

| # | What I did | What happened | Root cause | Fix |
| --- | --- | --- | --- | --- |
| 1 | Init git, pushed skeleton | `src refspec main does not match any` | No commits existed yet (a branch only exists after commit 1) | `git add . && git commit -m ...` then push |
| 2 | `uv venv` + install deps | — | — | `uv venv --python 3.12 && uv pip install -r requirements.txt` |
| 3 | First Spark test with `spark.jars.packages` pointing to Maven | Cell ran 8+ min, no output | Spark was downloading Delta jars at runtime; download stalled silently | Stop relying on runtime downloads; jars must be local |
| 4 | Downloaded delta jars manually into `jars/`, set `spark.jars` | `ClassNotFoundException: delta.DefaultSource` | Notebook CWD wasn't project root → glob `jars/*.jar` matched nothing → Spark started without Delta | Use absolute paths derived from a known root |
| 5 | Retried | `HADOOP_HOME and hadoop.home.dir are unset` | Spark on Windows needs Hadoop's `winutils.exe` for file-permission checks — undocumented prerequisite | Download winutils, set `HADOOP_HOME` |
| 6 | Installed winutils.exe | `UnsatisfiedLinkError: NativeIO$Windows.access0` | winutils.exe is only a wrapper; the actual native code is in `hadoop.dll`, which was missing | Download `hadoop.dll` into `%HADOOP_HOME%\bin` |
| 7 | Added hadoop.dll, set env vars with `setx` | Same error again | `setx` writes to registry but running processes (Jupyter server) keep their old environment snapshot; `java.library.path` config is unreliable on Windows | Set `os.environ[...]` inside the notebook **before** Spark starts |
| 8 | Verified DLL with `ctypes.CDLL("C:\\hadoop\\bin\\hadoop.dll")` | `DLL loads OK` | Proved the file was good — pure discovery problem | — |
| 9 | Considered System32 copy + WSL2/Databricks fallback | — | — | Not needed after step 10 |
| 10 | **Final working setup** | All checks pass | Earlier Java install had a Windows networking/selector issue killing Spark; VS Code couldn't access runtime under `C:\Users\...` | Moved runtime to `F:\SparkRuntime`; set `JAVA_HOME`, `HADOOP_HOME`, `PATH` in notebook before Spark; used `configure_spark_with_delta_pip`; `local[1]` + `shuffle.partitions=1` for tiny tests |

## The Final Working Pattern (what's in `src/common/spark_session.py`)

1. Load `.env` file (each machine's own paths — never hardcode machine paths in source)
2. Append `%HADOOP_HOME%\bin` to `PATH` if `HADOOP_HOME` is set (Windows only)
3. Build SparkSession with Delta extensions + `configure_spark_with_delta_pip(builder)` —
this installs Delta jars **via pip**, no Maven, no manual jars
4. Small defaults for local dev: `spark.sql.shuffle.partitions=8`

## Lessons (so you don't relearn them)

- **Errors stack.** Each fix revealed the next error. It felt like one endless
problem; it was ~6 independent small ones. Diagnose one at a time.
- **`configure_spark_with_delta_pip` is the clean way** to get Delta jars — it
removed the entire `jars/` folder and Maven problem from the project.
- **Env vars set via `setx` don't reach already-running processes.** Set them
in-process before the JVM starts, or fully restart the terminal/kernel.
- **Python 3.13 is incompatible with PySpark 3.5** — pin 3.12 in the venv even
if 3.13 is your system Python.
- **Never hardcode absolute machine paths** in shared code — `.env` per machine,
paths derived from `__file__` in source.
- **Every notebook starts with the same 4 lines** (sys.path + project root) so
imports work regardless of where the notebook is saved.