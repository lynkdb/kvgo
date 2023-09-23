#!/bin/bash

set -e

go test -run=Test_AdminAPI .

go test -run=Test_TableReplica_API .

go test -run=Test_TableJob_LogPull .

go test -run=Test_TableJob_Clean .

go test -run=Test_ServiceApi_Base .

go test -run=Test_ServiceApi_RepX .

