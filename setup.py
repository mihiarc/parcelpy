from setuptools import setup, find_packages

setup(
    name="gee-lcms",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "geopandas",
        "pandas",
        "earthengine-api",
        "plotly",
        "matplotlib",
        "contextily",
        "numpy",
        "pyyaml",
        "python-dotenv",
    ],
    python_requires='>=3.7',
) 