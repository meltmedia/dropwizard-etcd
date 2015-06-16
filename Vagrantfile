# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
  end
  config.vm.network :forwarded_port, guest: 2379, host: 2379
  config.vm.provision :shell, :path => "provision.sh"
end
