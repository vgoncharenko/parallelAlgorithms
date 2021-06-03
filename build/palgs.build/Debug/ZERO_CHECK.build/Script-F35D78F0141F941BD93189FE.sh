#!/bin/sh
set -e
if test "$CONFIGURATION" = "Debug"; then :
  cd /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build
  make -f /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build/CMakeScripts/ReRunCMake.make
fi
if test "$CONFIGURATION" = "Release"; then :
  cd /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build
  make -f /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build/CMakeScripts/ReRunCMake.make
fi
if test "$CONFIGURATION" = "MinSizeRel"; then :
  cd /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build
  make -f /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build/CMakeScripts/ReRunCMake.make
fi
if test "$CONFIGURATION" = "RelWithDebInfo"; then :
  cd /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build
  make -f /Users/vitaliy/Documents/magento/cpp/parallelAlgorithms/build/CMakeScripts/ReRunCMake.make
fi

