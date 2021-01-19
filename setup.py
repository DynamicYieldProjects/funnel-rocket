import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="funnel-rocket",
    version="0.0.7",
    author="Elad Rosenheim, Avshalom Manevich",
    author_email="elad@dynamicyield.com",
    description="Cloud native distributed funnel queries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DynamicYieldProjects/funnel-rocket",
    packages=setuptools.find_packages(),
    package_data={
        "frocket": ["resources/*.*"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",  # TODO Classify version
        # "License :: OSI Approved :: Apache Software License",  # TODO Pending approval!
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=['pyarrow',
                      'pandas',
                      'boto3',
                      'redis',
                      'tabulate',
                      'prometheus_client',
                      'flask',
                      'jsonschema',
                      'dataclasses-json',
                      'inflection',
                      'parsimonious']
)
