%define name replicatord
%define _rel      1
License:          Proprietary
Vendor:           Mail.Ru
Group:            RB
URL:              https://confluence.mail.ru/display/RB
Source0:          %{name}-%{current_datetime}.tar.gz
BuildRoot:        %{_tmppath}/%{name}-%{current_datetime}
Name:             %{name}
Version:          1.0.12
Release:          %{_rel}%{version_suffix}
Group:            Applications/Databases
BuildRequires:    gcc, make, gcc-c++, zeromq_rb-devel, boost-devel
BuildRequires:    cmake >= 2.8
Requires:         boost-system, boost-serialization, zeromq_rb
%if 0%{rhel} < 7
BuildRequires:    mysql-devel
Requires:         mysql-libs
Requires:         mailru-wrapper
%else
Buildrequires:    mysql-devel >= 1:5.5.42
Requires:         mysql-libs >= 1:5.5.42
Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd
BuildRequires:    systemd
%endif
Summary:          Mysql to Tarantool replication daemon
%description
Package contains Mysql to Tarantool replication daemon
Git version: %{git_version} (branch: %{git_branch})

%define __bindir        /usr/local/sbin
%define __etcdir        /usr/local/etc

%prep
%{__rm} -rf %{buildroot}
%setup -n %{name}-%{current_datetime}

%build
cd ..
cd %{name}-%{current_datetime}
cmake . && make

%install
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}
%{__mkdir} -p %{buildroot}/usr
%{__mkdir} -p %{buildroot}/usr/local/sbin

%if 0%{rhel} >= 7
    %{__mkdir} -p %{buildroot}/usr/lib/systemd/system/
    %{__install} -pD -m 644 %{name}.service %{buildroot}/usr/lib/systemd/system/%{name}.service
%else
    %{__mkdir} -p %{buildroot}/etc/rc.d/init.d
    %{__install} -m 755 rc.%{name} %{buildroot}/etc/rc.d/init.d/%{name}
%endif

make %{_smp_mflags} CFLAGS="$RPM_OPT_FLAGS" DESTDIR=$RPM_BUILD_ROOT INSTALLDIRS=vendor install

%post
%if 0%{rhel} >= 7
%systemd_post %{name}.service
%endif

%preun
%if 0%{rhel} >= 7
%systemd_preun %{name}.service
%endif

%postun
%if 0%{rhel} >= 7
%systemd_postun
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
/usr/local/sbin/%{name}
%config(noreplace) /usr/local/etc/%{name}.cfg
%if 0%{rhel} >= 7
    /usr/lib/systemd/system/%{name}.service
%else
    /etc/rc.d/init.d/%{name}
%endif
