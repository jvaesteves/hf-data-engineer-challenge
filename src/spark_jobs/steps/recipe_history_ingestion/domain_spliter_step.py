from pyspark.sql.functions import col, regexp_extract
from pyspark.sql import DataFrame
from typing import Dict, List

from spark_jobs.base_classes import SparkStep

class DomainSpliterStep(SparkStep):

    @property
    def url_domain_extraction_regex(self) -> str: 
        return r'^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)'

    def validate_url_domains(self, df: DataFrame, domains: List[str]):
        domain_occurrences = df.groupBy('url_domain').count().collect()
        domain_occurrences = { url_domain: count for url_domain, count in domain_occurrences }

        for domain in domains:
            domain_count = domain_occurrences.pop(domain, None)
            if domain_count:
                self.log.info(f"Processing {domain_count} rows for '{domain}' domain")
            else:
                self.log.warn(f'No occurrences of domain: {domain}')

        unknown_domains = []
        for domain, domain_count in domain_occurrences.items():
            unknown_domains.append(domain)
            self.log.error(f"Found {domain_count} rows for a not registered domain: '{domain}'")

        if unknown_domains:
            raise Exception(f'Found not registered domains: {unknown_domains}')
    
    def split_dataframe_by_url_domains(self, df: DataFrame, url_column_name: str, domains: List[str]) -> Dict[str, DataFrame]:
        self.log.info(f"Splitting input by the following URL domains: {domains}")

        df = df.withColumn('url_domain', regexp_extract(col(url_column_name), self.url_domain_extraction_regex, 1))

        self.validate_url_domains(df, domains)

        df_partitions = { 
            domain: df.where(col('url_domain').like(f'%{domain}')).drop('url_domain')
            for domain in domains
        }
        
        return df_partitions

    def run(self, df: DataFrame, url_column_name: str, domains: List[str]) -> Dict[str, DataFrame]:
        return self.split_dataframe_by_url_domains(df, url_column_name, domains)
