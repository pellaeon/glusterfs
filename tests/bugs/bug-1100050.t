#!/bin/bash

. $(dirname $0)/../include.rc
. $(dirname $0)/../volume.rc

cleanup;

TEST glusterd;
TEST pidof glusterd;

TEST $CLI volume create $V0 stripe 2 $H0:$B0/{1,2} force;
TEST $CLI volume start $V0;
TEST glusterfs --volfile-server=$H0 --volfile-id=$V0 $M0;

TEST $CLI volume quota $V0 enable;

EXPECT_WITHIN $PROCESS_UP_TIMEOUT "Y" quotad_up_status;

TEST mkdir $M0/dir;

TEST $CLI volume quota $V0 limit-usage /dir 10MB;

TEST mkdir $M0/dir/subdir;

cleanup;
