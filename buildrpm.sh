#!/bin/sh
set -xe

SPECFILE=$1

err() {
  exitval="$1"
  shift
  echo "$@" > /dev/stderr
  exit $exitval
}

echo "Building \"$1\""
if [ ! -f "$1" ]; then
  err 1 "Spec \"$1\" not found"
fi

shift

GIT_VERSION="$(git rev-list HEAD -n 1)"
EXTENDED_VERSION="$(git log -n 1 --pretty=format:'%h (%ai)')"
BRANCH="$(git name-rev --name-only HEAD)"
BRANCH_FOR_RPM="$(echo $BRANCH | rev | cut -d/ -f1 | rev | sed 's/-/_/g')"
PACKAGER="$(git config user.name) <$(git config user.email)>"
LAST_COMMIT_DATETIME="$(git log -n 1 --format='%ci' | awk '{ print $1, $2 }' | sed 's/[ :]//g;s/-//g')"
CURRENT_DATETIME=`date +'%Y%m%d%H%M%S'`

if [ ! -f "$HOME/.rpmmacros" ]; then
   echo "%_topdir $HOME/rpm/" > $HOME/.rpmmacros
   echo "%_tmppath $HOME/rpm/tmp" >> $HOME/.rpmmacros
   echo "%packager ${PACKAGER}" >> $HOME/.rpmmacros

fi
if [ ! -d "$HOME/rpm" ]; then
  echo "Creating directories need by rpmbuild"
  mkdir -p ~/rpm/{BUILD,RPMS,SOURCES,SRPMS,SPECS,tmp} 2>/dev/null
  mkdir ~/rpm/RPMS/{i386,i586,i686,noarch} 2>/dev/null
fi

RPM_TOPDIR=`rpm --eval '%_topdir'`
BUILDROOT=`rpm --eval '%_tmppath'`
BUILDROOT_TMP="$BUILDROOT/tmp/"
BUILDROOT="$BUILDROOT/tmp/${PACKAGE}"

PACKAGE=replicatord-${CURRENT_DATETIME}
[ "$PACKAGE" != "/" ] && [ -d "$PACKAGE" ] && rm -rf "$PACKAGE"

mkdir -p ${RPM_TOPDIR}/{BUILD,RPMS,SOURCES,SRPMS,SPECS}
mkdir -p ${RPM_TOPDIR}/RPMS/{i386,i586,i686,noarch}
mkdir -p $BUILDROOT

git archive --format=tar --prefix=${PACKAGE}/ ${BRANCH}| gzip > ${RPM_TOPDIR}/SOURCES/${PACKAGE}.tar.gz

echo '############################################################'

VERSION_SUFFIX="%{nil}" # this protects against RPM's error "macro has empty body"
if [ "${BRANCH_FOR_RPM}" != "master" ]; then
  VERSION_SUFFIX=".${BRANCH_FOR_RPM}.${LAST_COMMIT_DATETIME}"
fi

rpmbuild -ba --clean $SPECFILE \
  --define "current_datetime ${CURRENT_DATETIME}" \
  --define "version_suffix ${VERSION_SUFFIX}" \
  --define "git_version ${GIT_VERSION}" \
  --define "git_branch ${BRANCH_FOR_RPM}"
