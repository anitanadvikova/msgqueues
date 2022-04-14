package com.study.crawler.service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class UrlCrawlerService {

    private final AmqpTemplate amqpTemplate;

    @RabbitListener(queues = "consume_queue")
    public void consumeLink(String message) {
        final List<String> links = getLinks(message);
        produceLink(links);
    }

    public void produceLink(List<String> links) {
        links.forEach(this::sendMessage);
    }


    private void sendMessage(String link) {
        amqpTemplate.convertAndSend("produce_queue", link);
    }

    @SneakyThrows
    private List<String> getLinks(String link) {
        String resultLink;
        if (link.contains("https://ru.wikipedia.org/")) {
            resultLink = link;
        } else {
            resultLink = "https://ru.wikipedia.org/" + link;
        }
        final Document document = Jsoup.connect(resultLink).get();
        return document.getElementsByTag("a")
                .stream()
                .map(element -> element.attr("href"))
                .collect(Collectors.toList());
    }

}
