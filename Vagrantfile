# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/xenial64"
  config.vm.provision "shell", path: "provision.sh"
  config.vm.hostname = "phoenix-test"
  config.vm.network "forwarded_port", guest: 8765, host: 8765
end
