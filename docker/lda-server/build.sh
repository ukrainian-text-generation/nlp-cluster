export GCLOUD_PROJECT="diploma-llm"
export REPO="nlp-repository"
export REGION="europe-west4"
export IMAGE="lda-server"

export IMAGE_TAG=${REGION}-docker.pkg.dev/$GCLOUD_PROJECT/$REPO/$IMAGE
docker build -t $IMAGE_TAG -f ./docker/lda-server/Dockerfile --platform linux/x86_64 .
docker push $IMAGE_TAG