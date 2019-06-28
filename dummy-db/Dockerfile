FROM golang:1.12 as build

# get and install dependencies
RUN go get -u github.com/kardianos/govendor

# project-specific information
ARG PROJECT_PATH=github.com/khagerma/stateful-experiment
ARG INJECT_PACKAGE_PATH=main
ARG MAIN_PATH=dummy-db/main.go

WORKDIR /go/src/$PROJECT_PATH

# copy in pinned code dependency file(s)
COPY vendor/vendor.json vendor/

# download/install pinned versions of dependencies
RUN govendor sync -v
RUN govendor install -v -tags netgo +vendor

# get working tree information from build script
ARG GIT_BRANCH
ARG GIT_COMMIT_NUM
ARG GIT_COMMIT
ARG GIT_UNCOMMITTED_CHANGES

# bring in source files
COPY . ./

# build static binary
RUN go build -v -tags netgo \
  --ldflags "-extldflags \"-static\" \
  -X \"$INJECT_PACKAGE_PATH.GIT_BRANCH=$GIT_BRANCH\" \
  -X \"$INJECT_PACKAGE_PATH.GIT_COMMIT_NUM=$GIT_COMMIT_NUM\" \
  -X \"$INJECT_PACKAGE_PATH.GIT_COMMIT=$GIT_COMMIT\" \
  -X \"$INJECT_PACKAGE_PATH.GIT_UNCOMMITTED_CHANGES=$GIT_UNCOMMITTED_CHANGES\"" \
  -o /go/bin/executable "$MAIN_PATH"



# create final container
FROM alpine:3.10

# add static dependencies
RUN apk --no-cache add ca-certificates

# copy final resources from last build stage
COPY --from=build /go/bin/executable /bin/executable

# startup command
CMD ["executable"]
