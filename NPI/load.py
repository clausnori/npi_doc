import re
import zipfile
import pandas as pd
from pandas import DataFrame
from typing import Optional, Dict, List, Generator, Any, Union
import os

"""
Initialization of the class for working with NPI data in a CSV or ZIP file

Args:

file_path (str): Path to the CSV or ZIP file

prefix (str): Prefix used to search for CSV files in the ZIP (ignored for standalone CSV)

csv_filename (str, optional): Specific name of the CSV file inside the ZIP (if known)
"""



class NPI_Load:
    def __init__(self, file_path: str, prefix: str = "", csv_filename: Optional[str] = None):
        self.file_path = file_path
        self.prefix = prefix.lower() if prefix else ""
        self.csv_filename = csv_filename
        self.is_zip = file_path.lower().endswith('.zip')
        
        # Valide file in init
        self._validate_file()
        
        if self.is_zip and not self.csv_filename:
            self._find_csv_file()
    
    def _validate_file(self) -> None:
        if not os.path.exists(self.file_path):
            raise ValueError(f"File not find: {self.file_path}")
        
        if self.is_zip:
            try:
                with zipfile.ZipFile(self.file_path, 'r') as z:
                    z.testzip()
            except (zipfile.BadZipFile, FileNotFoundError) as e:
                raise ValueError(f"Non correct Zip: {self.file_path}. Exp: {e}")
        else:
            if not self.file_path.lower().endswith('.csv'):
                raise ValueError(f"Only for CVS or Zip: {self.file_path}")
    
    def _find_csv_file(self) -> None:
        with zipfile.ZipFile(self.file_path, 'r') as z:
            csv_files = [
                f for f in z.namelist() 
                if f.lower().endswith('.csv') and f.lower().startswith(self.prefix)
            ]
            
            if not csv_files:
                #if not find prefix , we just pick first file
                all_csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
                if all_csv_files:
                    self.csv_filename = all_csv_files[0]
                    print(f"File which '{self.prefix}' Non find. Use: {self.csv_filename}")
                else:
                    raise ValueError(f"CSV Not find in Zip \n Set file_patch in method: {self.file_path}")
            else:
                self.csv_filename = csv_files[0]
                print(f"CVS File Find: {self.csv_filename}")
    
    def _get_file_handle(self):
        if self.is_zip:
            zip_file = zipfile.ZipFile(self.file_path, 'r')
            csv_file = zip_file.open(self.csv_filename)
            return zip_file, csv_file
        else:
            return None, open(self.file_path, 'r', encoding='utf-8')
    
    def read_csv_head(self, n: int = 10) -> pd.DataFrame:
        """DEV method for check file structure 
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
            print(f"Read {n} in : {filename}")
            df_head = pd.read_csv(csv_file, nrows=n)
            return df_head
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def get_schema_from_sample(self, sample_size: int = 100) -> Dict[str, str]:
        """
        Get fillds in CSV 
        
        Args:
            sample_size (int): Size for analysis 
            
        Returns:
            Dict[str, str]: Type and Name
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            df_sample = pd.read_csv(csv_file, nrows=sample_size)
            schema = {col: str(dtype) for col, dtype in df_sample.dtypes.items()}
            return schema
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def read_csv_in_chunks(self, 
                          chunk_size: int = 100_000, 
                          dtype_map: Optional[Dict[str, Any]] = None, 
                          date_cols: Optional[List[str]] = None) -> Generator[pd.DataFrame, None, None]:
        """
        chunks CSV
        
        Args:
            chunk_size (int): Size one part
            dtype_map (Dict, optional): Field type
            date_cols (List[str], optional): List collons for parse
            
        Yields:
            pd.DataFrame: Part Data
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=chunk_size,
                dtype=dtype_map,
                parse_dates=date_cols,
                low_memory=False
            )
            
            for i, chunk in enumerate(chunk_iter):
                print(f"Chunk {i + 1}: {len(chunk)} len")
                yield chunk
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def read_full_csv(self, 
                     dtype_map: Optional[Dict[str, Any]] = None, 
                     date_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Чтение всего CSV файла целиком
        
        Args:
            dtype_map (Dict, optional): Словарь типов данных для колонок
            date_cols (List[str], optional): Список колонок для парсинга дат
            
        Returns:
            pd.DataFrame: Полный DataFrame
        """
        zip_file, csv_file = self._get_file_handle()
        
        try:
            filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
            print(f"Чтение полного файла: {filename}")
            df = pd.read_csv(
                csv_file,
                dtype=dtype_map,
                parse_dates=date_cols,
                low_memory=False
            )
            return df
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
    
    def find_npi(self, 
                 npi_number: Union[str, int], 
                 npi_column: str = 'NPI',
                 chunk_size: int = 100_000,
                 dtype_map: Optional[Dict[str, Any]] = None,
                 return_first: bool = True) -> pd.DataFrame:
        """
        Поиск записей по NPI номеру в CSV файле
        
        Args:
            npi_number (Union[str, int]): NPI номер для поиска
            npi_column (str): Название колонки с NPI номерами (по умолчанию 'NPI')
            chunk_size (int): Размер чанка для чтения больших файлов
            dtype_map (Dict, optional): Словарь типов данных для колонок
            return_first (bool): Если True, вернуть сразу после первого найденного совпадения
            
        Returns:
            pd.DataFrame: DataFrame с найденными записями
        """
        # Конвертируем NPI в строку для универсальности
        npi_str = str(npi_number).strip()
        
        filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
        print(f"Поиск NPI {npi_str} в файле: {filename}")
        
        # Результирующий DataFrame
        result_df = pd.DataFrame()
        
        # Проверяем, есть ли колонка NPI в файле
        zip_file, csv_file = self._get_file_handle()
        try:
            # Читаем только заголовки для проверки
            header_df = pd.read_csv(csv_file, nrows=0)
            if npi_column not in header_df.columns:
                available_cols = list(header_df.columns)
                raise ValueError(f"Колонка '{npi_column}' не найдена в файле. Доступные колонки: {available_cols}")
        finally:
            csv_file.close()
            if zip_file:
                zip_file.close()
        
        # Поиск по чанкам
        found_count = 0
        total_chunks = 0
        
        for chunk in self.read_csv_in_chunks(chunk_size=chunk_size, dtype_map=dtype_map):
            total_chunks += 1
            
            # Конвертируем колонку NPI в строку для сравнения
            chunk[npi_column] = chunk[npi_column].astype(str)
            
            # Ищем совпадения
            matches = chunk[chunk[npi_column] == npi_str]
            
            if not matches.empty:
                found_count += len(matches)
                print(f"Найдено {len(matches)} совпадений в чанке {total_chunks}")
                
                # Добавляем к результату
                if result_df.empty:
                    result_df = matches.copy()
                else:
                    result_df = pd.concat([result_df, matches], ignore_index=True)
                
                # Если нужно вернуть сразу после первого совпадения
                if return_first:
                    print(f"Поиск завершен досрочно. Найдена первая запись в чанке {total_chunks}.")
                    return result_df
        
        print(f"Поиск завершен. Обработано {total_chunks} чанков.")
        print(f"Всего найдено записей: {found_count}")
        
        if result_df.empty:
            print(f"NPI номер {npi_str} не найден в файле.")
        
        return result_df
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Get CVS Data information 
        
        Returns:
            Dict: Data information 
        """
        info = {
            'file_path': self.file_path,
            'is_zip': self.is_zip,
            'file_size': os.path.getsize(self.file_path)
        }
        
        if self.is_zip:
            info['csv_filename'] = self.csv_filename
            info['prefix'] = self.prefix
            
            with zipfile.ZipFile(self.file_path, 'r') as z:
                zip_info = z.getinfo(self.csv_filename)
                info.update({
                    'file_size_compressed': zip_info.compress_size,
                    'file_size_uncompressed': zip_info.file_size,
                    'compression_type': zip_info.compress_type,
                    'date_time': zip_info.date_time
                })
        else:
            info['csv_filename'] = os.path.basename(self.file_path)
        
        return info