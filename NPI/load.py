import re
import zipfile
import pandas as pd
from pandas import DataFrame
from typing import Optional, Dict, List, Generator, Any, Union
import os
import io

"""
Optimized class for working with NPI data in a CSV or ZIP file with memory-efficient loading

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
        
        # Cache for zip file handle to avoid repeated opening
        self._zip_file_handle = None
        self._schema_cache = None
        
        # Validate file in init
        self._validate_file()
        
        if self.is_zip and not self.csv_filename:
            self._find_csv_file()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        self.close()
    
    def close(self):
        """Explicitly close any open file handles"""
        if self._zip_file_handle:
            self._zip_file_handle.close()
            self._zip_file_handle = None
    
    def _validate_file(self) -> None:
        if not os.path.exists(self.file_path):
            raise ValueError(f"File not found: {self.file_path}")
        
        if self.is_zip:
            try:
                # Quick validation without loading entire file
                with zipfile.ZipFile(self.file_path, 'r') as z:
                    # Just check if it's a valid zip, don't test all contents
                    z.namelist()
            except (zipfile.BadZipFile, FileNotFoundError) as e:
                raise ValueError(f"Invalid ZIP file: {self.file_path}. Error: {e}")
        else:
            if not self.file_path.lower().endswith('.csv'):
                raise ValueError(f"Only CSV or ZIP files supported: {self.file_path}")
    
    def _find_csv_file(self) -> None:
        with zipfile.ZipFile(self.file_path, 'r') as z:
            # Get only CSV files without reading content
            csv_files = [
                f for f in z.namelist() 
                if f.lower().endswith('.csv') and not f.startswith('__MACOSX/') and '/' not in f.strip('/')
            ]
            
            if self.prefix:
                # Filter by prefix
                prefix_files = [f for f in csv_files if f.lower().startswith(self.prefix)]
                if prefix_files:
                    self.csv_filename = prefix_files[0]
                    print(f"CSV file found with prefix: {self.csv_filename}")
                    return
            
            if not csv_files:
                raise ValueError(f"No CSV files found in ZIP: {self.file_path}")
            
            # Use first CSV file if no prefix match
            self.csv_filename = csv_files[0]
            if self.prefix:
                print(f"File with prefix '{self.prefix}' not found. Using: {self.csv_filename}")
            else:
                print(f"CSV file found: {self.csv_filename}")
    
    def _get_zip_handle(self):
        """Get cached zip file handle to avoid repeated opening"""
        if not self._zip_file_handle or self._zip_file_handle.fp.closed:
            self._zip_file_handle = zipfile.ZipFile(self.file_path, 'r')
        return self._zip_file_handle
    
    def _get_csv_stream(self):
        """Get CSV stream without keeping zip handle open unnecessarily"""
        if self.is_zip:
            zip_handle = self._get_zip_handle()
            return zip_handle.open(self.csv_filename)
        else:
            return open(self.file_path, 'r', encoding='utf-8')
    
    def read_csv_head(self, n: int = 10) -> pd.DataFrame:
        """
        DEV method for checking file structure with minimal memory usage
        """
        csv_stream = self._get_csv_stream()
        
        try:
            filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
            print(f"Reading {n} rows from: {filename}")
            
            # Read only what we need
            df_head = pd.read_csv(csv_stream, nrows=n, low_memory=False)
            return df_head
        finally:
            if not self.is_zip:  # Only close if it's a direct file handle
                csv_stream.close()
    
    def get_schema_from_sample(self, sample_size: int = 100) -> Dict[str, str]:
        """
        Get field types from CSV sample with caching
        
        Args:
            sample_size (int): Sample size for analysis 
            
        Returns:
            Dict[str, str]: Column names and their data types
        """
        # Return cached schema if available
        if self._schema_cache:
            return self._schema_cache
        
        csv_stream = self._get_csv_stream()
        
        try:
            df_sample = pd.read_csv(csv_stream, nrows=sample_size, low_memory=False)
            schema = {col: str(dtype) for col, dtype in df_sample.dtypes.items()}
            self._schema_cache = schema  # Cache for future use
            return schema
        finally:
            if not self.is_zip:
                csv_stream.close()
    
    def read_csv_in_chunks(self, 
                          chunk_size: int = 100_000, 
                          dtype_map: Optional[Dict[str, Any]] = None, 
                          date_cols: Optional[List[str]] = None,
                          use_columns: Optional[List[str]] = None) -> Generator[pd.DataFrame, None, None]:
        """
        Memory-efficient chunked CSV reading
        
        Args:
            chunk_size (int): Size of each chunk
            dtype_map (Dict, optional): Column data types
            date_cols (List[str], optional): Columns to parse as dates
            use_columns (List[str], optional): Only load specified columns
            
        Yields:
            pd.DataFrame: Data chunk
        """
        csv_stream = self._get_csv_stream()
        
        try:
            chunk_iter = pd.read_csv(
                csv_stream,
                chunksize=chunk_size,
                dtype=dtype_map,
                parse_dates=date_cols,
                low_memory=False,
                usecols=use_columns  # Only load needed columns
            )
            
            for i, chunk in enumerate(chunk_iter):
                print(f"Processing chunk {i + 1}: {len(chunk)} rows")
                yield chunk
                
        finally:
            if not self.is_zip:
                csv_stream.close()
    
    def read_full_csv(self, 
                     dtype_map: Optional[Dict[str, Any]] = None, 
                     date_cols: Optional[List[str]] = None,
                     use_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Read full CSV with memory optimizations
        
        Args:
            dtype_map (Dict, optional): Column data types
            date_cols (List[str], optional): Columns to parse as dates
            use_columns (List[str], optional): Only load specified columns
            
        Returns:
            pd.DataFrame: Complete DataFrame
        """
        csv_stream = self._get_csv_stream()
        
        try:
            filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
            print(f"Reading full file: {filename}")
            
            df = pd.read_csv(
                csv_stream,
                dtype=dtype_map,
                parse_dates=date_cols,
                low_memory=False,
                usecols=use_columns
            )
            return df
        finally:
            if not self.is_zip:
                csv_stream.close()
    
    def find_npi(self, 
                 npi_number: Union[str, int], 
                 npi_column: str = 'NPI',
                 chunk_size: int = 100_000,
                 dtype_map: Optional[Dict[str, Any]] = None,
                 return_first: bool = True,
                 return_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Memory-efficient NPI search with early termination
        
        Args:
            npi_number (Union[str, int]): NPI number to search for
            npi_column (str): Name of the column with NPI numbers
            chunk_size (int): Chunk size for reading large files
            dtype_map (Dict, optional): Column data types
            return_first (bool): Return immediately after first match
            return_columns (List[str], optional): Only return specified columns
            
        Returns:
            pd.DataFrame: DataFrame with found records
        """
        npi_str = str(npi_number).strip()
        
        filename = self.csv_filename if self.is_zip else os.path.basename(self.file_path)
        print(f"Searching for NPI {npi_str} in file: {filename}")
        
        # Validate column exists by reading header only
        header_df = self.read_csv_head(n=0)  # Read just headers
        if npi_column not in header_df.columns:
            available_cols = list(header_df.columns)
            raise ValueError(f"Column '{npi_column}' not found. Available columns: {available_cols}")
        
        # Determine which columns to load
        use_columns = None
        if return_columns:
            use_columns = list(set([npi_column] + return_columns))  # Include NPI column for search
        
        result_df = pd.DataFrame()
        found_count = 0
        
        # Optimize dtype for NPI column
        if dtype_map is None:
            dtype_map = {}
        dtype_map[npi_column] = str  # Ensure NPI is treated as string
        
        for chunk in self.read_csv_in_chunks(
            chunk_size=chunk_size, 
            dtype_map=dtype_map,
            use_columns=use_columns
        ):
            # Direct string comparison without additional conversion
            matches = chunk[chunk[npi_column] == npi_str]
            
            if not matches.empty:
                found_count += len(matches)
                
                # Filter return columns if specified
                if return_columns:
                    matches = matches[return_columns]
                
                if result_df.empty:
                    result_df = matches.copy()
                else:
                    result_df = pd.concat([result_df, matches], ignore_index=True)
                
                if return_first:
                    print(f"Found {len(matches)} record(s) for NPI {npi_str}")
                    return result_df
        
        if result_df.empty:
            raise ValueError(f"NPI number {npi_str} not found in file")
        
        print(f"Found {found_count} total record(s) for NPI {npi_str}")
        return result_df
    
    def search_by_criteria(self,
                          criteria: Dict[str, Any],
                          chunk_size: int = 100_000,
                          dtype_map: Optional[Dict[str, Any]] = None,
                          return_columns: Optional[List[str]] = None,
                          max_results: Optional[int] = None) -> pd.DataFrame:
        """
        Memory-efficient search by multiple criteria
        
        Args:
            criteria (Dict[str, Any]): Search criteria {column: value}
            chunk_size (int): Chunk size for processing
            dtype_map (Dict, optional): Column data types
            return_columns (List[str], optional): Columns to return
            max_results (int, optional): Maximum number of results
            
        Returns:
            pd.DataFrame: Matching records
        """
        print(f"Searching with criteria: {criteria}")
        
        # Validate columns exist
        header_df = self.read_csv_head(n=0)
        missing_cols = [col for col in criteria.keys() if col not in header_df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found: {missing_cols}")
        
        # Determine columns to load
        use_columns = None
        if return_columns:
            search_cols = list(criteria.keys())
            use_columns = list(set(search_cols + return_columns))
        
        result_df = pd.DataFrame()
        total_found = 0
        
        for chunk in self.read_csv_in_chunks(
            chunk_size=chunk_size,
            dtype_map=dtype_map,
            use_columns=use_columns
        ):
            # Apply all criteria
            mask = pd.Series([True] * len(chunk), index=chunk.index)
            for col, value in criteria.items():
                mask &= (chunk[col].astype(str) == str(value))
            
            matches = chunk[mask]
            
            if not matches.empty:
                # Filter return columns if specified
                if return_columns:
                    matches = matches[return_columns]
                
                if result_df.empty:
                    result_df = matches.copy()
                else:
                    result_df = pd.concat([result_df, matches], ignore_index=True)
                
                total_found += len(matches)
                
                # Check if we've reached max results
                if max_results and total_found >= max_results:
                    result_df = result_df.head(max_results)
                    break
        
        print(f"Found {len(result_df)} record(s) matching criteria")
        return result_df
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Get CSV file information without loading content
        
        Returns:
            Dict: File information 
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
                    'compression_ratio': round(zip_info.compress_size / zip_info.file_size * 100, 2),
                    'compression_type': zip_info.compress_type,
                    'date_time': zip_info.date_time
                })
        else:
            info['csv_filename'] = os.path.basename(self.file_path)
        
        return info
    
    def get_column_info(self, sample_size: int = 1000) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed column information with minimal memory usage
        
        Args:
            sample_size (int): Sample size for analysis
            
        Returns:
            Dict: Column information including type, nulls, unique values
        """
        csv_stream = self._get_csv_stream()
        
        try:
            df_sample = pd.read_csv(csv_stream, nrows=sample_size, low_memory=False)
            
            column_info = {}
            for col in df_sample.columns:
                column_info[col] = {
                    'dtype': str(df_sample[col].dtype),
                    'null_count': df_sample[col].isnull().sum(),
                    'null_percentage': round(df_sample[col].isnull().sum() / len(df_sample) * 100, 2),
                    'unique_count': df_sample[col].nunique(),
                    'sample_values': df_sample[col].dropna().head(3).tolist()
                }
            
            return column_info
        finally:
            if not self.is_zip:
                csv_stream.close()