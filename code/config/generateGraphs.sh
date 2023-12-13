#!/usr/bin/env bash
# ----
# Script to generate the detail graphs of a Vodka run.
#
# Copyright (C) 2023 Hu Zirui
# ----

if [ $# -lt 1 ] ; then
    echo "usage: $(basename $0) RESULT_DIR [SKIP_MINUTES]" >&2
    exit 2
fi

if [ $# -gt 1 ] ; then
	SKIP=$2
else
	SKIP=0
fi

WIDTH=12
HEIGHT=6
POINTSIZE=12

#SIMPLE_GRAPHS="latency "
SIMPLE_GRAPHS="tpm_nopm latency_new cpu_utilization dirty_buffers memory"
#SIMPLE_GRAPHS="latency_new "
# dirty_buffers
resdir="$1"
cd "${resdir}" || exit 1
for graph in $SIMPLE_GRAPHS ; do
	echo -n "Generating ${resdir}/${graph}.svg ... "
	out=$(sed -e "s/@WIDTH@/${WIDTH}/g" \
		  -e "s/@HEIGHT@/${HEIGHT}/g" \
		  -e "s/@POINTSIZE@/${POINTSIZE}/g" \
		  -e "s/@SKIP@/${SKIP}/g" \
		  </home/xjk/hzr/Vodka-Benchmark/run/misc/${graph}.R | R --no-save)
	if [ $? -ne 0 ] ; then
		echo "ERROR"
		echo "$out" >&2
		exit 3
	fi
	echo "OK"
done

# 定义你的地址列表
addresses=("'xjk@49.52.27.33'" "'xjk@49.52.27.34'" "'xjk@49.52.27.35'")
for dir in $(ls ./data); do
  # 如果目录下有 CSV 文件
  # 判断是否存在 blk_*.csv 文件
      devname=$(ls ./data/${dir}/blk_*.csv 2> /dev/null)
      if [ $? -eq 0 ]; then
          for address in "${addresses[@]}"; do
            devname=$(basename "$devname" .csv)
            echo -n "Generating ${resdir}/${address}_iops.svg ... "
            # 使用 sed 来替换 R 脚本中的参数，并调用 R 脚本
            out=$(sed -e "s#@WIDTH@#${WIDTH}#g" \
                       -e "s#@HEIGHT@#${HEIGHT}#g" \
                       -e "s#@POINTSIZE@#${POINTSIZE}#g" \
                       -e "s#@SKIP@#${SKIP}#g" \
                       -e "s#@ADDRESS@#${address}#g" \
                       -e "s#@DEVICE@#${devname}#g" </home/xjk/hzr/Vodka-Benchmark/run/misc/blk_device_iops.R | R --no-save)
            if [ $? -ne 0 ] ; then
              echo "ERROR"
              echo "$out" >&2
              exit 3
            fi

            echo "OK"
            echo -n "Generating ${resdir}/${devname}_kbps.svn ... "
            out=$(sed -e "s#@WIDTH@#${WIDTH}#g" \
                                -e "s#@HEIGHT@#${HEIGHT}#g" \
                                -e "s#@POINTSIZE@#${POINTSIZE}#g" \
                                -e "s#@SKIP@#${SKIP}#g" \
                                -e "s#@ADDRESS@#${address}#g" \
                                -e "s#@DEVICE@#${devname}#g" </home/xjk/hzr/Vodka-Benchmark/run/misc/blk_device_kbps.R | R --no-save)
            if [ $? -ne 0 ] ; then
              echo "ERROR"
              echo "$out" >&2
              exit 3
            fi
           echo "OK"
          done
      fi
      # 判断是否存在 net_*.csv 文件
      devname=$(ls ./data/${dir}/net*.csv 2> /dev/null)
      if [ $? -eq 0 ]; then
      	for address in "${addresses[@]}"; do
          devname=$(basename "$devname" .csv)
          echo -n "Generating ${resdir}/${devname}_iops.svn ... "
          out=$(sed -e "s#@WIDTH@#${WIDTH}#g" \
              -e "s#@HEIGHT@#${HEIGHT}#g" \
              -e "s#@POINTSIZE@#${POINTSIZE}#g" \
              -e "s#@SKIP@#${SKIP}#g" \
              -e "s#@ADDRESS@#${address}#g" \
              -e "s#@DEVICE@#${devname}#g" </home/xjk/hzr/Vodka-Benchmark/run/misc/net_device_iops.R | R --no-save)
          if [ $? -ne 0 ] ; then
            echo "ERROR"
            echo "$out" >&2
            exit 3
          fi
          echo "OK"

          echo -n "Generating ${resdir}/${devname}_kbps.svn ... "
          out=$(sed -e "s#@WIDTH@#${WIDTH}#g" \
              -e "s#@HEIGHT@#${HEIGHT}#g" \
              -e "s#@POINTSIZE@#${POINTSIZE}#g" \
              -e "s#@SKIP@#${SKIP}#g" \
              -e "s#@ADDRESS@#${address}#g" \
              -e "s#@DEVICE@#${devname}#g" </home/xjk/hzr/Vodka-Benchmark/run/misc/net_device_kbps.R | R --no-save)
          if [ $? -ne 0 ] ; then
            echo "ERROR"
            echo "$out" >&2
            exit 3
          fi
          echo "OK"
      done
    fi
done
cd ..
#
