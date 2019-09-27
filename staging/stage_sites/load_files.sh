#!/usr/bin/env bash

SID=$1
SUPPORT_CTL=$2
EXT=$3
export SID SUPPORT_CTL EXT


if [ $# -lt 1 ] ; then

echo Arguments should be: \<sid\> \<SUPPORT_CTL\> \<EXT\>
else

# Get the username/password for given SID
read -p "Username for $1: " USERNAME
read -s -p "Password for $1: " PASSWORD

for i in $SUPPORT_CTL/*.$EXT; do
   if [ -r $i ]; then
     s=$i
     s=${s##*/}
     j=${s%.$EXT}
     echo $j
     # Invoke sqlldr
     echo sh -c "sqlldr $USERNAME/$PASSWORD@\\$SID control=$SUPPORT_CTL/$j.ctl skip=1 log=$SUPPORT_CTL/$j.log bad=$SUPPORT_CTL/$j.bad data=$i ERRORS=2000"
   fi
  done
fi
