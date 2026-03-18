
import shutil
import os

# 외부 파일 data 폴더로 복붙
# source_dir = 원본파일 있는 폴더
# target_dir = 프로젝트 안에 data폴더
# file_name = 원본 폴더 중 원하는 파일명
def file_copy(source_dir, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for filename in os.listdir(source_dir):
        if not filename.endswith('.csv'):
            continue
        source_path = os.path.join(source_dir, filename)
        target_path = os.path.join(target_dir, filename)
        if os.path.exists(target_path):
            print('이미 존재하는 파일입니다.')
        else:
            shutil.copy(source_path, target_path)
            print(f'{filename} 복사가 완료되었습니다.')

# 각 변수 값만 바꿔서 쓰시면 됩니다.
source_dir = r'D:\IDE\shellfolder\download'
file_copy(source_dir,'./data')

