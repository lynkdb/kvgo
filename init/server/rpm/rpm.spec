%define app_home /opt/lynkdb/kvgo
%define app_user kvgo

Name: kvgo-server
Version: __version__
Release: __release__%{?dist}
Vendor:  lynkdb.com
Summary: lynkdb key-value database server
License: Apache 2

Source0: %{name}-__version__.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}

Requires: redhat-lsb-core
Requires(pre): shadow-utils

%description

%prep
%setup -q -n %{name}-%{version}
%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}%{app_home}/bin
mkdir -p %{buildroot}%{app_home}/etc
mkdir -p %{buildroot}%{app_home}/var/log
mkdir -p %{buildroot}%{app_home}/var/data
mkdir -p %{buildroot}%{app_home}/init/server
mkdir -p %{buildroot}/lib/systemd/system

cp -rp init/server/* %{buildroot}%{app_home}/init/server/
install -m 755 bin/kvgo-server %{buildroot}%{app_home}/bin/kvgo-server
install -m 600 init/server/systemd/systemd.service %{buildroot}/lib/systemd/system/kvgo-server.service

%clean
rm -rf %{buildroot}

%pre
getent passwd %{app_user} >/dev/null || \
    useradd -d %{app_home} -s /sbin/nologin %{app_user}
exit 0

%post
systemctl daemon-reload

%preun

%postun

%files
%defattr(-,kvgo,kvgo,-)
%dir %{app_home}
/lib/systemd/system/kvgo-server.service

%{app_home}/

