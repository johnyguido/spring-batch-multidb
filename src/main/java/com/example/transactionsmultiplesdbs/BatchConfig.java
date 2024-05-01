package com.example.transactionsmultiplesdbs;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class BatchConfig {

  @Autowired
  @Qualifier("springDS")
  private DataSource dataSourceMeta;

  @Autowired
  @Qualifier("appDS")
  private DataSource dataSourceApp;

  @Autowired
  @Qualifier("transactionManagerApp")
  private PlatformTransactionManager transactionManager;

  @Autowired
  @Qualifier("transactionManagerMeta")
  private PlatformTransactionManager transactionManagerMeta;

  @Bean
  public Job job(JobRepository jobRepository, Step step) {
    try {
      System.out.println("PERIOD DE PAUSA " + 10000);
      Thread.sleep(10000); // Pausa por 10 segundos (ajuste conforme necessário)
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return new JobBuilder("job", jobRepository)
            .start(step)
            .incrementer(new RunIdIncrementer())
            .listener(metadataCleanupListener())
            .build();
  }

  @Bean
  public Step step(ItemReader<Pessoa> reader, ItemWriter<Pessoa> writer, JobRepository jobRepository) {
    return new StepBuilder("step", jobRepository)
        .<Pessoa, Pessoa>chunk(200, this.transactionManager)
        .reader(reader)
        .writer(writer)
        .build();
  }

  @Bean
  public ItemReader<Pessoa> reader() {
    return new FlatFileItemReaderBuilder<Pessoa>()
        .name("reader")
        .resource(new FileSystemResource("files/pessoas.csv"))
        .comments("--")
        .delimited()
        .names("nome", "email", "dataNascimento", "idade", "id")
        .targetType(Pessoa.class)
        .build();
  }

  @Bean
  public ItemWriter<Pessoa> writer(@Qualifier("appDS") DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Pessoa>()
        .dataSource(dataSource)
        .sql(
            "INSERT INTO pessoa (id, nome, email, data_nascimento, idade) VALUES (:id, :nome, :email, :dataNascimento, :idade)")
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .build();
  }

  @Bean
  public JdbcTemplate jdbcTemplate() {
    return new JdbcTemplate(this.dataSourceMeta);
  }

  // Cria uma instância do MetadataCleanupListener
  @Bean
  public MetadataCleanupListener metadataCleanupListener() {
    return new MetadataCleanupListener(jdbcTemplate());
  }


  public class MetadataCleanupListener implements JobExecutionListener {

    private final JdbcTemplate jdbcTemplate;

    // Injeta o JdbcTemplate
    public MetadataCleanupListener(JdbcTemplate jdbcTemplate) {
      this.jdbcTemplate = jdbcTemplate;
    }

    // Executa antes da execução do job
    @Override
    public void beforeJob(JobExecution jobExecution) {
      try {
        System.out.println("PERIOD DE PAUSA PARA EXCLUSAO DE METADADOS " + 10000);
        Thread.sleep(15000); // Pausa por 10 segundos (ajuste conforme necessário)
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      // Executa o script SQL para limpar as tabelas de metadados do Spring Batch
      jdbcTemplate.execute("DELETE FROM BATCH_STEP_EXECUTION_CONTEXT");
      jdbcTemplate.execute("DELETE FROM BATCH_STEP_EXECUTION");
      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION_CONTEXT");
      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION_PARAMS");
      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION");
      jdbcTemplate.execute("DELETE FROM BATCH_JOB_INSTANCE");
      jdbcTemplate.execute("DELETE FROM BATCH_STEP_EXECUTION_SEQ");
      jdbcTemplate.execute("DELETE FROM BATCH_JOB_EXECUTION_SEQ");
      jdbcTemplate.execute("DELETE FROM BATCH_JOB_SEQ");
    }

    // Executa após a execução do job (não utilizado neste exemplo)
    @Override
    public void afterJob(JobExecution jobExecution) {
      // Nada a fazer aqui
    }
  }

}
