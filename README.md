### Build and push base python.project image

docker build -t python.project -f Dockerfile.base .
docker tag python.project adrianmarino/python.project:lastest
docker push adrianmarino/python.project:lastest

### Build and push concret project image

docker build -t test.project .
docker tag test.project adrianmarino/test.project:lastest
docker push adrianmarino/test.project:lastest
