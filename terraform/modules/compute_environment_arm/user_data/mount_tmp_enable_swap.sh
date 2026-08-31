MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/x-shellscript; charset="us-ascii"
########################################
# NOTES
# ECS optimized AMIs come with a second EBS volume used by Docker
# These volumes are mounted on /dev/nvme1n1 but don't show up in the launch template
# Ephemeral storage is mounted on dev/nvme2n1 and up
# Number of storage devices differ between instance types
# For this script is meant to work with ECS optimized AMI
# and instance types with at least 1 ephemeral storage devices ie r5d, c5d etc.
# For instance with two and more ephemeral storage devices (r5d.4xlarge/ c5d.12xlarge and up)
# the second device will be used as swap drive
#
# Device naming is identical on Nitro-based Graviton (arm64) instances, so
# this script is unchanged from the x86_64 compute_environment module.
#########################################

#!/bin/bash
yum install -y rsync

#######################################
# Mount the ephemeral storage
#######################################

mkfs.ext4 /dev/nvme2n1
mkdir -p /mnt/ext
mount -t ext4 /dev/nvme2n1 /mnt/ext

# make temp directory for containers usage
# should be used in the Batch job definition (MountPoints)
mkdir /mnt/ext/tmp
rsync -avPHSX /tmp/ /mnt/ext/tmp/

# modify fstab to mount /tmp on the new storage.
sed -i '$ a /mnt/ext/tmp  /tmp  none  bind  0 0' /etc/fstab
mount -a

# make /tmp usable by everyone
chmod 777 /mnt/ext/tmp

########################################
# Create swap space
# but only if 2nd drive exists
########################################

if ls /dev/nvme* | grep -q '/dev/nvme3n1'; then
  # Set up a Linux swap area on the device with the mkswap command.
  mkswap /dev/nvme3n1

  # Enable the new swap space.
  swapon /dev/nvme3n1

  # Edit your /etc/fstab file so that this swap space is automatically enabled at every system boot.
  # (propbably not nessecessary)
  sed -i '$ a /dev/nvme3n1  none  swap  sw  0 0' /etc/fstab

fi

#########################################
# Finishing up
#########################################

# Create 0 byte file "READY" to allow processes to check if new volume is ready for use
touch /mnt/ext/tmp/READY

--==MYBOUNDARY==--
