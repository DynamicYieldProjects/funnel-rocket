# Packaging a Lambda Layer
##This is out of date, update it!

See: [AWS Lambda Layers docs](https://docs.aws.amazon.com/lambda/latest/dg/configuration-layers.html)

Layers are a convenient way to package a function dependencies once and upload them to AWS as a kind of "base image", so that function deployment packages remain small.

Typically, creating a .zip file with a few Python packages is easy. The added complexity here is that we use packages with native code compiled to different OS variants, with potentially different optimizations.
To avoid compatibility issues due to the package being installed locally, we install the packages in a Docker container with the Amazon Linux 2 based image.
Then, we zip the packages as they were installed in that OS - the same as Lambdas run.

#### The build process

Follows the [workflow in this post](https://towardsdatascience.com/how-to-install-python-packages-for-aws-lambda-layer-74e193c76a91
).

The build process has two phases:
1. Build the Amazon Linux 2 image with Python 3.8 - can be re-used for multiple layers.
2. Based on that image, build an image with the relevant dependencies installed.
Then, we copy a zip file of these dependencies back into the host machine.
