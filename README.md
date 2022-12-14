<meta name="robots" content="noindex">

# MorphStreamDR

- "R" stands for fast durability and recovery 
- "D" stands for distributed (D) transactional stream processing engine

This project aims at 
(i) design fast durability and recovery mechanism for MorphStream,
(ii)building a distributed (D) transactional (T) stream processing engine with CXL-enabled reliable storage backend.
Compute Express Link (CXL) which is an industry-supported cache-coherent interconnect for processors, memory expansion and
accelerators. CXL enables cacheable load/store (ld/st) accesses to memory on Intel, AMD, and ARM processors at nanosecond-
scale latency. CXL access via loads/stores is a game changer to design systems with pooling and sharing memory.


## How to Cite MorphStream

If you use MorphStream in your paper, please cite our work.

* **[ICDE]** Shuhao Zhang, Bingsheng He, Daniel Dahlmeier, Amelie Chi Zhou, Thomas Heinze. Revisiting the design of data stream processing systems on multi-core processors, ICDE, 2017 (code: https://github.com/ShuhaoZhangTony/ProfilingStudy)
* **[SIGMOD]** Shuhao Zhang, Jiong He, Chi Zhou (Amelie), Bingsheng He. BriskStream: Scaling Stream Processing on Multicore Architectures, SIGMOD, 2019 (code: https://github.com/Xtra-Computing/briskstream)
* **[ICDE]** Shuhao Zhang, Yingjun Wu, Feng Zhang, Bingsheng He. Towards Concurrent Stateful Stream Processing on Multicore Processors, ICDE, 2020
* **[xxx]** We have an anonymized submission under review. Stay tuned.
```
@INPROCEEDINGS{9101749,  
author={Zhang, Shuhao and Wu, Yingjun and Zhang, Feng and He, Bingsheng},  
booktitle={2020 IEEE 36th International Conference on Data Engineering (ICDE)},   
title={Towards Concurrent Stateful Stream Processing on Multicore Processors},   
year={2020},  
volume={},  
number={},  
pages={1537-1548},  
doi={10.1109/ICDE48307.2020.00136}
}

```
