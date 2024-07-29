export GCLOUD_PROJECT="diploma-llm"
export REPO="nlp-repository"
export REGION="europe-west4"
export IMAGE="dependency-parser"

export IMAGE_TAG=${REGION}-docker.pkg.dev/$GCLOUD_PROJECT/$REPO/$IMAGE
docker build -t $IMAGE_TAG -f ./docker/dependency-parser/Dockerfile --platform linux/x86_64 .
docker push $IMAGE_TAG