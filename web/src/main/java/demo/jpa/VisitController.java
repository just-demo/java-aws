package demo.jpa;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/visits")
public class VisitController {
    @Autowired
    private VisitRepository visitRepository;

    @GetMapping
    private Iterable<Visit> findAll() {
        visitRepository.save(new Visit());
        return visitRepository.findAll();
    }
}
