# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
    config.vm.box = "bento/ubuntu-16.04"

    # Create a forwarded port mapping which allows access to a specific port
    # within the machine from a port on the host machine. In the example below,
    # accessing "localhost:5000" will access port 5000 on the guest machine.
    config.vm.network "forwarded_port", guest: 5000, host: 5000, auto_correct: true

    config.vm.provider "virtualbox" do |vb|
      # Customize the amount of memory on the VM:
      vb.memory = "2048"
    end

    #Run the shell script inline provisioner
    config.vm.provision "shell", path: "vagrant-setup.sh", privileged: "false"
end
