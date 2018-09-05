# Homework Setup

This document should help you set up the environment needed to do
assignments in CS 186.

In order to ensure that your homework solutions are
compatible with the CS186 grading infrastructure, we need to enable you to use the same software environment that we use for grading.

To that end, we require that homeworks are implemented to execute correctly inside a [docker
container](https://www.docker.com/resources/what-container), for which we
are providing an image. (Note on terminology: a docker `image` is a 
static object; a docker `container` is a running instance of an image.) You will be able to 
run this image on Mac, Windows
or Linux computers. The image itself is an Ubuntu Linux environment 
with a bash shell. We assume you know the basics of bash and UNIX commands. 

**The cs186 docker image is the supported coding environment for this course.** You are free to put together your own toolchain if you like in a different environment, but (a) we will not help you if you run into problems, and (b) if our autograders provide different output than your environment, you could lose points on your grades, and we will not offer leniency on that front. Hence we strongly encourage you to exclusively work with the docker image for developing and testing your homework.

## Installing Docker 
The class docker image is called `cs186/environment.` Before you can use it, you need to install Docker Community Edition ("CE"). On your machine. Most recent laptops should support Docker CE.

- To install Docker CE on Mac or Windows, open the
[Docker getting started page](https://www.docker.com/get-started),
stay on the "Developer" tab, and click the buttonon the right to download the
installer for your OS. Follow instructions included. 
- To install Docker CE on Linux, open the [Docker
Guides](https://docs.docker.com/), and using the menu on the left
navigate to Get Docker->Docker CE->Linux to find instructions for your
Linux distro.

### Additional Notes for Windows Users
1. Not all editions of Windows support the default `Docker for Windows` distribution. To quote from [the Docker docs](https://docs.docker.com/docker-for-windows/install/):

>The current version of Docker for Windows runs on 64bit Windows 10 Pro, Enterprise and Education (1607 Anniversary Update, Build 14393 or later).

2. If you are running another version of Windows, you should be able to get Docker running by downloading [Docker Toolbox](https://docs.docker.com/toolbox/overview/). (Alternatively, if you'd like to upgrade your computer, [UC Berkeley students can install Windows 10 Education for free](https://software.berkeley.edu/microsoft-os)).
3. When we refer to a "terminal" here, it can be
a traditional `Command Prompt`, or `PowerShell`.  However,
interactive docker terminals do not work in `PowerShell ISE`, only
in `PowerShell`. 
4. If you are a Windows
Linux Subsystem user (you would know if you are), then there are
various blog posts  [like this one](https://nickjanetakis.com/blog/setting-up-docker-for-windows-and-wsl-to-work-flawlessly)  that will
show you how to set WSL to play nicely with Docker for Windows.
5. You need to run Docker from a Windows account with Administrator privileges.

## Getting the class docker image
The next step is to get the class docker
image. To do this, *get on a good internet connection*, open a
terminal on your computer and run

``` docker pull cs186/environment ```

This will download the class image onto your computer. When it
completes, try the following to make sure everything is working:

``` docker run cs186/environment echo "hello from cs186" ```

That should print `hello from cs186` on your terminal. It did this by
running the `echo` shell command inside a `cs186/environment` docker
container.  In future you probably want to run a shell like bash, in interactive mode:
``` 
docker run -it cs186/environment bash
``` 
After some notifications, you should get a prompt like this:
```
ubuntu@1891ee9ee645:/$
```
Don't forget the `-it` flag; you need it to get docker to give you an interactive prompt.
When you are done with the shell and want to return to your
terminal (host environment) type ``` exit ``` or ```ctrl + D``` at the ubuntu prompt.

If you are curious to learn more about using Docker, there is [a nice
tutorial online](https://docker-curriculum.com/).

## Sharing your computer's drive with Docker containers
An important thing to realize about Docker is that every time you exit your container, it discards the changes you made to its filesystem state. That means that **you must not store your files inside the Docker container's filesystem:  it will delete them when you exit!**

Instead, you will store your files in your own computer's "local" drive, and *mount* a directory from your local drive within Docker. Mounted volumes in Docker exist outside the Docker container, and hence are not reset when Docker exits.

### Configuring support for shared drives
You may need to configure your Docker installation to share local drives, depending on your OS. Set this up now.

- **Linux**: you can skip this section -- nothing for you to worry about!
- **Mac**: be aware that you can only share directories under `/Users/`, `/Volumes/`, `/private` and `/tmp/` by default. If that's inconvenient for you, this is [configurable](https://docs.docker.com/docker-for-mac/osxfs/#namespaces).
- **Windows (Docker for Windows)**: To configure Docker to share local drives, follow the instructions [here](https://docs.docker.com/docker-for-windows/#shared-drives). The pathname you will need to use in the `docker run -v` command will need to include a root-level directory name corresponding to the Windows drive letter, and UNIX-style forward slashes. E.g. for the Windows directory `C:\\Users\myid` you would use `docker run -v /c/Users/myid`.
- **Windows (Docker Toolbox)**: be aware that you can only share data within `C:\Users` by default. If the `C:\Users` directory is inconvenient for you, there are [workarounds](http://support.divio.com/local-development/docker/how-to-use-a-directory-outside-cusers-with-docker-toolbox-on-windowsdocker-for-windows) you can try at your own risk. **Also:** the pathname you will need to use in the `docker run -v` command will start with `//c/Users/`. Note the leading double-forward-slash, which is different than Docker for Windows!

### Mounting your shared drive
Your homework files should live on your machine, *not within a Docker container directory!*. To expose a directory from your machine's filesystem to your docker container, you will `docker run` with the `-v` flag as follows:

    docker run -v <pathname-to-directory-on-your-machine>:/cs186 -it cs186/environment bash

(Remember if you're running Docker Toolbox on Windows, `<pathname-to-directory-on-your-machine>` should start with `//c/Users/`.) 

This mounts your chosen directory to appear under docker at /cs186. When you get a prompt from docker, simply `cd` to that directory and you should see your local files. 

    ubuntu@95b2c8583144:/$ cd /cs186
    ubuntu@95b2c8583144:/cs186$ ls
    <your files listed here>

Now you can edit those files within docker *and any changes you make in that directory subtree will persist across docker invocations* because the files are stored on your machine's filesystem, and not inside the docker container.

### Backing up and versioning your work
We **strongly** encourage you to plan to back up your files using a system that keeps multiple versions. *You would be crazy not to have a plan for this! We will not be helping you manage backups, this is your responsibility!*

The hacker's option here is to [learn `git`](http://git-scm.com/book/en/v1/Getting-Started) well enough to manager your own repository. 
However, since we are not working in teams this semester, it may be sufficient for your purposes to use an automatic desktop filesync service like Box, OneDrive or Dropbox. UC Berkeley students have access to free Box.com accounts as documented [here](https://bconnected.berkeley.edu/collaboration-services/box). There may be some hiccups making sure that your sync software works with Docker shared drives; for Box users we recommend using the older [Box Sync](https://community.box.com/t5/Using-Box-Sync/Installing-Box-Sync/ta-p/85) instead of the newer Box Drive application.

Whatever backup scheme you use, make sure that your files are not publicly visible online. For example, github users should make sure their repos are private.

### Using Your Favorite Desktop Tools
Because your files live on your machine's filesystem (in `<pathname-to-directory-on-your-machine>`), you can use your favorite editor or other desktop tools to modify those files. Any changes you save to those files on your machine will be instantly reflected in Docker. As a result, you can think of the docker image as a place to build and run your code, not a place to *edit* your code!

**Windows users:** you might need to be aware that Windows convention (inherited from DOS) ends lines of text differently than UNIX/Mac convention (see [this blog post](https://blog.codinghorror.com/the-great-newline-schism/) for fun history). This could make your code look odd inside the docker image and not run properly. If you run into this problem, you may need to configure your editor to generate UNIX-style newlines.

## `git` and GitHub

`git` is a *version control* system, that helps developers like you
track different versions of your code, synchronize them across
different machines, and collaborate with others.
[GitHub](https://github.com) is a site which supports this system,
hosting it as a service.

We will only be using git and GitHub to pass out homework assignments
in this course. If you don't know much about git, that isn't a
problem: you will *need* to use it only in very simple ways that we will
show you in order to keep up with class assignments.

Your class docker container includes git already, so you do not need
to install it on your machine separately if you do not want to.

If you'd like to use git for managing your own code versions, there are many guides to using git online -- 
[this](http://git-scm.com/book/en/v1/Getting-Started) is a good one.

## "Cloning" the main CS186 git repo

All assignments in CS 186 will be passed out via GitHub. [Our main
course repository is visible online here](https://github.com/berkeley-cs186/course);
you have read-only access. We will post assignments and updates on GitHub
in this repo as well as in per-assignment repos.
Please check Piazza to keep up-to-date on changes to assignments.

For now, let's get started by getting you a local copy of the CS186 main git repo.
To do this, we open a bash
shell **in your docker container** (see instructions above), **`cd` to
the shared directory from your local drive** (i.e. `/cs186`) so your files persist when you exit docker, and clone the repo as follows:

    $ docker run -v <pathname-to-directory-on-your-machine>:/cs186 -it cs186/environment bash
    ubuntu@95b2c8583144:/$ cd /cs186
    ubuntu@95b2c8583144:/cs186$ git clone https://github.com/berkeley-cs186/course.git


You should see a few lines of output from git. When it's done, try this: 

    ubuntu@95b2c8583144:cs186$ cd course 
    ubuntu@95b2c8583144:cs186/course$ ls 

You should see the files for the course assignments
released so far. You can compare to the [github website for the
repo](https://github.com/berkeley-cs186/course) to confirm it worked
properly.

When you exit docker, you should find the checked-out files on your machine in the directory `<pathname-to-directory-on-your-machine>` that you used in your `docker run` command.

### Receiving new assignments and assignment updates

We will release new assignments by registering them as commits in github repositories. From time to time, it may also be
necessary to `pull` updates to our assignments (even though we try to
release them as "perfectly" as possible the first time). Assuming you
followed the previous steps, you can simply run the following command
to receive new assignments and/or updates:

    ubuntu@95b2c8583144:/$ cd /cs186/course
    ubuntu@95b2c8583144:/cs186/course$ git pull

