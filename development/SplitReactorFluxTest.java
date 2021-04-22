package fr.ght1pc9kc.baywatch;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.List;

@Slf4j
public class SplitReactorFluxTest {
    @Test
    void should_split_flux_with_groupby() {
        Flux<String> actual = Flux.fromIterable(List.of(
                CsvLine.of("DELETE", "1", "data 1"),
                CsvLine.of("UPDATE", "2", "data 2"),
                CsvLine.of("UPDATE", "3", "data 3"),
                CsvLine.of("INSERT", "4", "data 4"),
                CsvLine.of("DELETE", "5", "data 5")
        )).groupBy(l -> l.type).flatMap(gf -> {
            switch (gf.key()) {
                case "DELETE":
                    return delete(gf);
                case "UPDATE":
                    return update(gf);
                case "INSERT":
                    return insert(gf);
                default:
                    return Flux.error(new IllegalArgumentException("Unknown key " + gf.key()));
            }
        });

        StepVerifier.create(actual)
                .expectNext(
                        "Delete data 1",
                        "Update data 2",
                        "Update data 3",
                        "Insert data 4",
                        "Delete data 5"
                ).expectComplete()
                .verify();
    }

    @Test
    void should_split_flux_with_sinks() {

        Sinks.Many<CsvLine> toDelete = Sinks.many().unicast().onBackpressureBuffer();
        Sinks.Many<CsvLine> toInsert = Sinks.many().unicast().onBackpressureBuffer();
        Sinks.Many<CsvLine> toUpdate = Sinks.many().unicast().onBackpressureBuffer();
        Sinks.Many<CsvLine> onError = Sinks.many().unicast().onBackpressureBuffer();

        Flux<String> deleted = delete(toDelete.asFlux());
        Flux<String> inserted = insert(toInsert.asFlux());
        Flux<String> updated = update(toUpdate.asFlux());
        Mono<Long> countErrors = onError.asFlux().count().doOnNext(c -> log.error("{} lines with errors", c));

        Flux<String> actual = Flux.fromIterable(List.of(
                CsvLine.of("DELETE", "1", "data 1"),
                CsvLine.of("UPDATE", "2", "data 2"),
                CsvLine.of("UPDATE", "3", "data 3"),
                CsvLine.of("INSERT", "4", "data 4"),
                CsvLine.of("DELETE", "5", "data 5")
        )).doOnNext(csvLine -> {
            switch (csvLine.type) {
                case "DELETE":
                    toDelete.tryEmitNext(csvLine);
                    break;
                case "UPDATE":
                    toUpdate.tryEmitNext(csvLine);
                    break;
                case "INSERT":
                    toInsert.tryEmitNext(csvLine);
                    break;
                default:
                    onError.tryEmitNext(csvLine);
            }
        }).doOnComplete(() -> {
            toDelete.tryEmitComplete();
            toUpdate.tryEmitComplete();
            toInsert.tryEmitComplete();
            onError.tryEmitComplete();
        }).then(countErrors)
                .thenMany(Flux.merge(deleted, inserted, updated));

        StepVerifier.create(actual)
                .expectNext(
                        "Delete data 1",
                        "Delete data 5",
                        "Insert data 4",
                        "Update data 2",
                        "Update data 3"
                ).expectComplete()
                .verify();
    }

    public Flux<String> delete(Flux<CsvLine> toBeDeleted) {
        return toBeDeleted.map(csvLine -> "Delete " + csvLine.data);
    }

    public Flux<String> update(Flux<CsvLine> toBeUpdated) {
        return toBeUpdated.map(csvLine -> "Update " + csvLine.data);
    }

    public Flux<String> insert(Flux<CsvLine> toBeInserted) {
        return toBeInserted.map(csvLine -> "Insert " + csvLine.data);
    }


    @Value
    @AllArgsConstructor(staticName = "of")
    public static class CsvLine {
        public final String type;
        public final String id;
        public final String data;
    }
}
