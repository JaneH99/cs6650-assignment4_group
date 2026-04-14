package config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;
import websocket.ChatWebSocketHandler;

/**
 * Registers ChatWebSocketHandler at /chat/{roomId}.
 * setAllowedOrigins("*") permits connections from the test-client and browsers.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

  private final ChatWebSocketHandler handler;

  public WebSocketConfig(ChatWebSocketHandler handler) {
    this.handler = handler;
  }

  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
    registry.addHandler(handler, "/chat/{roomId}")
        .setAllowedOrigins("*");
  }

  @Bean
  public ServletServerContainerFactoryBean createWebSocketContainer() {
    ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
    container.setMaxSessionIdleTimeout(600000L); // 10 minutes — prevents idle disconnects during load tests
    container.setMaxTextMessageBufferSize(65536);   // 64 KB — prevents TEXT_PARTIAL_WRITING state
    container.setMaxBinaryMessageBufferSize(65536);
    return container;
  }
}
