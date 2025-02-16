#!/bin/bash
# filepath: /home/arjunr/Desktop/data-engineering-zoomcamp-2025/01-docker-terraform/1_terraform_gcp/terraform_with_variables/install_docker.sh

# Update the package list
apt-get update


# Install Docker Engine
apt-get install -y docker.io

# Add the current user to the docker group
usermod -aG docker arjunr

# Enable and start the Docker service
systemctl enable docker
systemctl start docker

git config --global user.name "arjunr"
git config --global user.email arjun121094@gmail.com
git clone https://github.com/arjunrarjunr/data-engineering-zoomcamp-2025.git /home/arjunr/de

