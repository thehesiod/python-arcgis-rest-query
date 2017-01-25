from setuptools import setup

install_requires = [
    "aiohttp>=1.2.0",
    "ujson"
]

setup(
    name='arcgis-rest-query',
    version='0.2',
    description='A tool to download a layer from an ArcGIS web service as GeoJSON',
    author='Ken Schwencke',
    author_email='schwank@gmail.com',
    url='https://github.com/Schwanksta/python-arcgis-rest-query',
    license='MIT',
    packages=('arcgis',),
    scripts=(
        'bin/arcgis-get',
    ),
    install_requires=install_requires,
)
