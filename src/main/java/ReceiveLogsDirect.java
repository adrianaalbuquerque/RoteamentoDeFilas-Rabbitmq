import com.rabbitmq.client.*;


public class ReceiveLogsDirect {

    private static final String EXCHANGE_NAME = "PDist_Roteamento_Filas";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("mqadmin");
        connectionFactory.setPassword("Admin123XX_");

        Connection connection = connectionFactory.newConnection();
        Channel canal = connection.createChannel();

        canal.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = canal.queueDeclare().getQueue();

        if (argv.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        for (String severity : argv) {
            canal.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Aguardando mensagens. Para sair, pressione CTRL + C");

        DeliverCallback callback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Recebido '"
                    + delivery.getEnvelope().getRoutingKey() + "':'" + mensagem + "'");
        };

        canal.basicConsume(queueName, true, callback, consumerTag -> {
        });

    };

};


