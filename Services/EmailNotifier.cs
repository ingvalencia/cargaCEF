using System;
using System.Net;
using System.Net.Mail;
using System.Threading.Tasks;

namespace SistemaConexionFirebird.Services
{
    public class EmailNotifier
    {
        private readonly string smtpServer;
        private readonly int smtpPort;
        private readonly string smtpUser;
        private readonly string smtpPass;
        private readonly bool enableSsl;

        // Constructor que configura los parámetros de SMTP
        public EmailNotifier(string smtpServer, int smtpPort, string smtpUser, string smtpPass, bool enableSsl = true)
        {
            this.smtpServer = smtpServer;
            this.smtpPort = smtpPort;
            this.smtpUser = smtpUser;
            this.smtpPass = smtpPass;
            this.enableSsl = enableSsl;
        }

        // Método que enviará correos electrónicos
        public async Task EnviarCorreoAsync(string destinatario, string asunto, string mensaje)
        {
            try
            {
                using (var smtpClient = new SmtpClient(smtpServer, smtpPort))
                {
                    smtpClient.Credentials = new NetworkCredential(smtpUser, smtpPass);
                    smtpClient.EnableSsl = enableSsl;

                    var mailMessage = new MailMessage
                    {
                        From = new MailAddress(smtpUser),
                        Subject = asunto,
                        Body = mensaje,
                        IsBodyHtml = true
                    };

                    mailMessage.To.Add(destinatario);

                    await smtpClient.SendMailAsync(mailMessage);
                    Console.WriteLine($"Correo enviado a: {destinatario}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al enviar el correo: {ex.Message}");
            }
        }
    }
}
