import pathlib
import setuptools

this_dir = pathlib.Path(__file__).parent
requirements_file = this_dir / "requirements.txt"
readme_file = this_dir / "README.md"

install_requires = requirements_file.read_text().splitlines()
long_description = readme_file.read_text() if readme_file.exists() else ''

setuptools.setup(
    name="funnel-rocket",
    version="0.5.2",
    author="Elad Rosenheim, Avshalom Manevich",
    author_email="elad@dynamicyield.com",
    description="Cloud native distributed funnel queries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DynamicYieldProjects/funnel-rocket-oss",
    packages=setuptools.find_packages(),
    package_data={
        "frocket": ["resources/*.*"],
    },
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        # "License :: OSI Approved :: Apache Software License",  # TODO Pending approval!
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=install_requires
)
