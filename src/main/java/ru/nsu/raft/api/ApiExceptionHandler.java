package ru.nsu.raft.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.concurrent.TimeoutException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<ApiError> timeout() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ApiError("Request timed out"));
    }

    @ExceptionHandler(InterruptedException.class)
    public ResponseEntity<ApiError> interrupted() {
        Thread.currentThread().interrupt();
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ApiError("Request interrupted"));
    }

    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ApiError> invalidRequest() {
        return ResponseEntity.badRequest().body(new ApiError("Invalid request body"));
    }

    public record ApiError(String message) {
    }
}
