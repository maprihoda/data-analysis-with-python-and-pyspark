Learning Pyspark locally (i.e. without using any cloud service) via following the excellent [Data Analysis with Python and PySpark](https://www.manning.com/books/data-analysis-with-python-and-pyspark) by Jonathan Rioux.

## Environment setup

From the project root, run:

```bash
pipenv install
```

This will create a virtual environment with all the required dependencies installed.

Although only ```pipenv``` is required for this setup to run, I strong recommend having both ```pyenv``` and ```pipenv``` installed. ```pyenv``` manages Python versions while ```pipenv``` takes care of virtual environments.

If you're on Windows, try [pyenv-win](https://github.com/pyenv-win/pyenv-win). ```pipenv``` should work just fine.

The notebooks were created with Visual Studio Code's [Jupyter code cells](https://code.visualstudio.com/docs/python/jupyter-support-py#_jupyter-code-cells), which I prefer over standard Jupyter notebooks/labs because of much better git integration.

You can easily convert the code cells files into Jupyter notebooks with Visual Studio Code. Just open a file, right click and select ```Export current Python file as Jupyter notebook```.

The ```data``` directory contains only the smaller-sized data files. You will have to download the larger ones as per the instructions in the individual notebooks, e.g.:

```bash
home_dir = os.environ["HOME"]
DATA_DIRECTORY = os.path.join(home_dir, "Documents", "spark", "data", "backblaze")
```

This works on my Linux machine. You may need to modify the path if you're on Windows.
