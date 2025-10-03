from Cython.Build import cythonize
from setuptools import setup

setup(
    name="netexsinglepy",
    py_modules=["domain.netex.model"],  # geef de module expliciet op
    ext_modules=cythonize("domain/netex/model.py", compiler_directives={"language_level": "3"}),  # dit bestand compileren
    script_args=["build_ext", "--inplace"],
)
