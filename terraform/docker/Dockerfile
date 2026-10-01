FROM globalforestwatch/terraform:v0.13.3

# The base image ships a docker CLI with no buildx plugin, so `docker build`
# falls back to the legacy builder and can't honor Dockerfiles that use
# BuildKit-only features like `--mount=type=cache`. This installs the buildx
# plugin so `DOCKER_BUILDKIT=1 docker build ...` works when this container
# talks to the host's Docker daemon over the mounted socket.
#
# The base image is amd64 so pin the buildx binary to linux-amd64 to match
RUN mkdir -p /root/.docker/cli-plugins \
  && curl -fsSL "https://github.com/docker/buildx/releases/download/v0.36.1/buildx-v0.36.1.linux-amd64" \
    -o /root/.docker/cli-plugins/docker-buildx \
  && chmod +x /root/.docker/cli-plugins/docker-buildx
