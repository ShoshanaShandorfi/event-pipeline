package dev.eventpipeline.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.math.BigDecimal;
import java.util.List;

@Configuration
@EnableMongoRepositories(basePackages = "dev.eventpipeline.service.repository")
public class MongoConfig {

    @Bean
    public MongoCustomConversions mongoCustomConversions() {
        return new MongoCustomConversions(List.of(
                new BigDecimalToDecimal128Converter(),
                new Decimal128ToBigDecimalConverter()));
    }

    @WritingConverter
    static class BigDecimalToDecimal128Converter implements Converter<BigDecimal, org.bson.types.Decimal128> {
        @Override
        public org.bson.types.Decimal128 convert(BigDecimal source) {
            return new org.bson.types.Decimal128(source);
        }
    }

    @ReadingConverter
    static class Decimal128ToBigDecimalConverter implements Converter<org.bson.types.Decimal128, BigDecimal> {
        @Override
        public BigDecimal convert(org.bson.types.Decimal128 source) {
            return source.bigDecimalValue();
        }
    }
}
