package robsyme.plugin

import nextflow.Session
import spock.lang.Specification

/**
 * Tests for TownCrier plugin
 */
class TownCrierObserverTest extends Specification {

    def 'should create observer when endpoint is configured' () {
        given:
        def session = Mock(Session) {
            getConfig() >> [towncrier: [endpoint: 'http://test.com/notify']]
        }
        def factory = new TownCrierFactory()

        when:
        def result = factory.create(session)

        then:
        result.size() == 1
        result.first() instanceof TownCrierObserver
    }

    def 'should not create observer when endpoint is missing' () {
        given:
        def session = Mock(Session) {
            getConfig() >> [towncrier: [:]]
        }
        def factory = new TownCrierFactory()

        when:
        def result = factory.create(session)

        then:
        result.isEmpty()
    }

    def 'should not create observer when explicitly disabled' () {
        given:
        def session = Mock(Session) {
            getConfig() >> [towncrier: [enabled: false, endpoint: 'http://test.com/notify']]
        }
        def factory = new TownCrierFactory()

        when:
        def result = factory.create(session)

        then:
        result.isEmpty()
    }

    def 'should not create observer when towncrier config is missing' () {
        given:
        def session = Mock(Session) {
            getConfig() >> [:]
        }
        def factory = new TownCrierFactory()

        when:
        def result = factory.create(session)

        then:
        result.isEmpty()
    }

    def 'should accept single process selector string' () {
        given:
        def session = Mock(Session) {
            getConfig() >> [towncrier: [endpoint: 'http://test.com/notify', processes: 'ALIGNMENT']]
        }
        def factory = new TownCrierFactory()

        when:
        def result = factory.create(session)

        then:
        result.size() == 1
    }

    def 'should accept list of process selectors' () {
        given:
        def session = Mock(Session) {
            getConfig() >> [towncrier: [endpoint: 'http://test.com/notify', processes: ['ALIGNMENT', 'MODKIT']]]
        }
        def factory = new TownCrierFactory()

        when:
        def result = factory.create(session)

        then:
        result.size() == 1
    }

}
