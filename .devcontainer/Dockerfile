# Image cho GitHub Codespaces (docs/cloud-codespaces.md).
# Image gốc có sẵn kho apt của Yarn mà khóa GPG không xác minh được (NO_PUBKEY 62D54FD4003F6525) → feature
# docker-in-docker chạy apt-get update bị lỗi exit 100 và codespace rơi vào recovery container (issue #1).
# Repo không dùng Yarn nên bỏ kho này trước khi các feature được cài.
FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm
RUN rm -f /etc/apt/sources.list.d/yarn.list
