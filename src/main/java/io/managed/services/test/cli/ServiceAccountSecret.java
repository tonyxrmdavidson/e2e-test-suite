package io.managed.services.test.cli;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ServiceAccountSecret {
    private String clientID;
    private String clientSecret;
}
