#!/bin/bash

set -e

go test -run=Test_AdminAPI .

go test -run=Test_DatabaseReplica_API .

go test -run=Test_DatabaseJob_LogPull .

go test -run=Test_DatabaseJob_Clean .

go test -run=Test_ServiceApi_Base .

go test -run=Test_ServiceApi_RepX .

