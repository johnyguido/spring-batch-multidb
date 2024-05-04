package com.example.transactionsmultiplesdbs;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import java.sql.SQLException;

@Configuration
public class DatasourceConfig {

  private static final Logger logger = LoggerFactory.getLogger(DatasourceConfig.class);

  @Primary
  @Bean
  @ConfigurationProperties(prefix = "mema.datasource")
  public DataSource springDS() {
    logger.info("ABRINDO CONEXAO: mema.datasource");
    return DataSourceBuilder.create().build();
  }


  @Bean
  @ConfigurationProperties(prefix = "spring.datasource")
  public DataSource appDS() {
    logger.info("ABRINDO CONEXAO: spring.datasource");
    return DataSourceBuilder.create().build();
  }

  @Bean
  @Qualifier("transactionManagerApp")
  public PlatformTransactionManager transactionManagerApp(@Qualifier("appDS") DataSource dataSource) throws SQLException {
    logger.info("ABRINDO CONEXAO: transactionManagerApp: {}", dataSource.getConnection());
    return new DataSourceTransactionManager(dataSource);
  }

  @Primary
  @Bean
  @Qualifier("transactionManagerMeta")
  public PlatformTransactionManager transactionManagerMeta(@Qualifier("springDS") DataSource dataSource) throws SQLException {
    logger.info("ABRINDO CONEXAO: transactionManagerMeta: {}", dataSource.getConnection());
    return new DataSourceTransactionManager(dataSource);
  }
}
