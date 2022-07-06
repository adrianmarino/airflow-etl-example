import torch
import pytorch_common.util as pu

pu.set_device_name('gpu')

print('CUDA:', torch.cuda.is_available())