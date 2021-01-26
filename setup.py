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
    url="https://github.com/DynamicYieldProjects/funnel-rocket-oss",
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
    install_requires=['pyarrow==2.0',
                      'pandas==1.2',
                      'boto3==1.16',
                      'redis==3.5.3',
                      'tabulate==0.8',
                      'prometheus_client==0.9',
                      'flask==1.1',
                      'jsonschema==3.2',
                      'dataclasses-json==0.5',
                      'inflection==0.5',
                      'parsimonious==0.8']
)
