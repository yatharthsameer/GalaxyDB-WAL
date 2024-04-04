# syntax=docker/dockerfile:1

FROM golang:1.21

RUN apt-get update \
    && apt-get install -y sudo apt-transport-https ca-certificates curl gnupg lsb-release \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get -y install docker-ce-cli

ENV USER=galaxydb
RUN useradd -m -s /bin/sh $USER \
    && usermod -aG sudo $USER

# Switch to the non-root user
USER root

RUN echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
    && chmod 0440 /etc/sudoers.d/$USER

WORKDIR /lb

# Copying the application source code
COPY . .

RUN chown -R $USER:$USER /lb
USER $USER

ENV GO_ENV=production

# build
RUN go mod download
RUN CGO_ENABLED=1 GOOS=linux go build -o /lb/galaxydb-lb -a -ldflags '-linkmode external -extldflags "-static"' .

# the application is going to listen in the port 5000
EXPOSE 5000

# run
CMD ["/lb/galaxydb-lb"]
