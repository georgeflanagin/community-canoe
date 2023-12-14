# -*- coding: utf-8 -*-
# Added for Python 3.5+
import typing
from typing import *

""" 
urmail is a module to support sending correctly formatted, short
messages to alert the recipients to some kind of problem. 
"""



import collections
import email
import imaplib
import os
import pdb
import smtplib

import canoeconfig
import canoelib
import tombstone as tomb
import urutils as uu

# Credits
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2015, University of Richmond'
__credits__ = None
__version__ = '0.1'
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'Prototype'


__license__ = 'MIT'
import license

from email.mime.text import MIMEText
OK = 'OK'

def test_tombstone(o:object) -> str:
    if __name__ == '__main__': return tomb.tombstone(o)
    else: pass


def resolve_email_addresses(to_list: list, 
        default_domain: str="richmond.edu", 
        phones: dict={}) -> list:
    """ 
    Take care of phone numbers, and account-only richmond.edu addresses. 
    """
    to_list = uu.listify(to_list)
    addresses = []

    for i in range(len(to_list)):
        phone = to_list[i]
        try:
            # Throw an exception if the address is non-numeric.
            _ = int(phone)
            try:
                addresses.append(phone + '@' + phones[phone])
            except:
                # For now, silent drop from the list. TODO: fix this.
                continue
        except ValueError:
            if phone.find('@') > 1: addresses.append(to_list[i])
            else: addresses.append(to_list[i] + '@' + default_domain)

    return addresses


def send_message(to_list: list, 
    gist: str,
    message: str, 
    conf_data: dict, 
    success: bool=True,
    subject: str='A message from Canoe', 
    from_address: str='canoe@richmond.edu') -> bool:

    """ 
    This function preps a message, and sends it out the door.

    to_list -- one or more addressees. If the argument is a string,
        it is made into a list with one member.
    gist -- the text shred with the serial number and recipe name.
    message -- a text shred (string) that is your message.
    conf_data -- dict containing info about the network and the
        configuration data for the mail gateways.
    success -- if this is a success message, then we can be low-key.
        Otherwise, try to get their attention.
    subject -- The default value is obtained from the config file
        that was loaded when the program started.
    from_address -- a string.

    NOTE: if items in the two list are strings of all digits, they
        are assumed to be phonenumbers, and the email is converted to
        an SMS for delivery.

    returns: -- True if all messages are sent, False otherwise.
    """

    # If there is no one to send it to, we are good.
    if not to_list: return True

    g = conf_data

    # g contains the email gateway, among many other things.
    # The following three lines support ad hoc testing.
    if not hasattr(g, 'email'):
        g.get_config(canoelib.get_canoe_home() + '/config/email.conf.json')
        if not hasattr(g, 'email'): return False

    # check the parameters
    if not from_address or not message:
        return uu.squeal("missing parameter in sendMessage(). Nothing sent.",source='canoe')


    # Expand .. take care of phone numbers.
    addresses = resolve_email_addresses(to_list, g.sys_params['default_domain'], g.phones)
    if not addresses: 
        tomb.tombstone("Nothing to send")
        return

    # open the gateway and send the message
    out = smtplib.SMTP(g.email["gateway"])
    if not out:
        uu.squeal("Unable to connect to gateway " + g.email["gateway"],source='canoe')
    try:
        message = MIMEText(gist + '\n' + message)
        gist = MIMEText(gist)
        message['Subject'] = subject
        message['From'] = from_address
        for _ in addresses:
            message['To'] = str(_)
            if _[0] in [str(_) for _ in range(0,10)]:
                tomb.tombstone("sending SMS to " + _)
                out.sendmail(from_address, _, gist.as_string())
                tomb.tombstone("message was " + gist.as_string())
            else:
                tomb.tombstone("sending regular mail to " + _)
                out.sendmail(from_address, _, message.as_string())
                tomb.tombstone("message was " + message.as_string())

    except smtplib.SMTPRecipientsRefused as e:
        tomb.tombstone("Some recipients in the following list were refused:")
        print(addresses)
        return True

    except smtplib.SMTPResponseException as e:
        print("Send failed. Code={1}. Explanation: {2}.",
            str(e.smtp_code),
            str(e.smtp_error))
        return False

    except:
        tomb.tombstone("Unknown error when sending mail.")
        return False
    
    return True


class IMAP_mailbox:
    """
    Many thanks to Yuji Tomita, whose working example for getting
    into GMail provided several insights about how do the work
    without having to read the entire RFC3501.

    The working example {wa|i}s here:

    https://yuji.wordpress.com/2011/06/22/python-imaplib-imap-example-with-gmail/
    """

    ALL='ALL'

    def __init__(self, config:dict):
        """
        Connect to a mailbox using the info in config.
        """
        self.box = None
        self.config = config
        self.messages = collections.OrderedDict()

        for key in ['hostname', 'user', 'password', 
                    'folder', 'readonly', 'download-to']:
            try:
                _ = config[key]
            except:
                tomb.tombstone(key + " is missing from mailbox config.")
                return

        try:
            self.box = imaplib.IMAP4_SSL(config['hostname'], port=993)
        except imaplib.IMAP4.error as e:
            tomb.tombstone(uu.type_and_text(e))
            try:
                self.box = imaplib.IMAP4(config['hostname'])
            except imaplib.IMAP4.error as e:
                tomb.tombstone(uu.type_and_text(e))
                self.box = None
                return
            else:
                tomb.tombstone('IMAP connection without SSL')
        else:
            tomb.tombstone('IMAP connection with SSL')

        try:
            self.box.login(config['user'], config['password'])
            self.box.select(config['folder'], readonly=config['readonly'])
        except imaplib.IMAP4.error as e:
            tomb.tombstone(uu.type_and_text(e))
            self.box = None


    def __bool__(self) -> bool:
        return self.box is not None


    def __del__(self):
        try:
            self.box.close()
        except Exception as e:
            pass

    def __str__(self) -> str:
        return str(self.box)


    def get_all_mail(self, *, oldest_first:bool=True) -> list:
        """
        A convenience method to get all the mail, oldest first as the only option.
        """
        return self.get_some_mail(IMAP_mailbox.ALL)


    def get_attachments(self, msg:object, download_folder:str="/tmp") -> list:
        """
        Given a message (as an object), save its attachments to the specified
        download folder (default is /tmp)

        return: a list of file paths to the attachments, or None
        """
        attachment_names = []
        for part in msg.walk():
            # Ignore things we are not looking for.
            if ( part.get_content_maintype() == 'multipart' or
                 part.get('Content-Disposition') is None ): continue

            attachment_name = os.path.join(download_folder, part.get_filename())
            test_tombstone('Found attachment ' + attachment_name)
            with open(attachment_name, 'wb+') as f:
                f.write(part.get_payload(decode=True))
                f.close()

            attachment_names.append(attachment_name)    

        return attachment_names or None


    """
    ... BEHOLD THE MIGHTY WORKHORSE ...
    """
    def get_some_mail(self, criteria:str="", *,
            unread_only:bool=True,
            oldest_first:bool=True,
            get_attachments:bool=True) -> collections.OrderedDict:
        """
        criteria -- something to look for .. subjects, senders, ...
        unread_only -- by default we only want new mail.
        oldest_first -- the contents come back in an /ordered/ dict, so
            whatever order is used in retrieval is the preserved. The
            uid-s of the messages are not guaranteed to be monotonically
            increasing. 
        get_attachments -- download any attachments.

        Retrieve the mail that matches `criteria`. The mail is retrieves as an
        ordereddict so that the keys are get-able in the order in which they are 
        inserted. Additionally, UIDs are used rather than message numbers so that
        messages may be removed/deleted without being subject to the ills of 
        renumbering.

        Each message becomes a dict, with all the keys that it has. Note that
        the available keys differ based on who sent the message, its format,
        and the mailer that was used.
        """

        def _get_message_body(message:object) -> str:
            """
            This is an internal method that simply avoids a looping structure
            while the message's body text is collected.
            """
            if message.is_multipart():
                return _get_message_body(message.get_payload(0))
            else:
                return message.get_payload(None, True)


        def _make_criteria(criteria:str) -> str:
            """
            We are searching the header for matching messages. Consequently,
            the criteria that are obtained from the recipe making this call
            must be reformatted. We want to go from 

            "subject=StarRez from=cbigler"

            to
    
            (HEADER Subject "StarRez" From "cbigler")
            """
            if criteria == IMAP_mailbox.ALL: return criteria

            criteria = uu.dictify(criteria)
            test_tombstone(str(criteria))
            result = "(HEADER "
            for k, v in criteria.items():
                result += k.title() + ' "' + str(v) + '" '
            result = result.strip() + ")"
            test_tombstone(result)
            return result


        test_tombstone(uu.fcn_signature('_make_criteria', criteria))
        result, payload = self.box.uid('search', None, _make_criteria(criteria))
        if result != OK: 
            tomb.tombstone('mailbox has no mail matching these criteria: ' + str(criteria))
            return self.messages

        # Decide how to order the results.
        message_ids = payload[0].split() if oldest_first else payload[0].split()[::-1]
        tomb.tombstone(str(len(message_ids)) + ' found.')

        tomb.tombstone('fetching messages ' + str(message_ids))
        for message_id in message_ids:
            result, payload = self.box.uid('fetch', message_id, '(RFC822)')
            if result == OK: 
                self.messages[message_id] = {}
                m = email.message_from_bytes(payload[0][1])
                for k in m.keys():
                    self.messages[message_id][k] = m[k]
                self.messages[message_id]['body'] = _get_message_body(m)

            if get_attachments:
                self.messages[message_id]['get_attachments'] = self.get_attachments(m, 
                                                            self.config['download-to'])

            else:   
                tomb.tombstone('woops!')        

        return self.messages


    def markasread(self, kwargs:str) -> bool:
        tomb.tombstone(kwargs)
        return True
        

    def read(self, kwargs:str) -> bool:
        if kwargs.strip().upper == IMAP_mailbox.ALL:
            return self.get_all_mail()
        return self.get_some_mail(kwargs)


if __name__ == "__main__":
    m = IMAP_mailbox({
    "hostname":"exchangemail.richmond.edu",
    "user":"canoetest",
    "folder":"inbox",
    "password":"prime1u=RashidovwV",
    "download-to":"/tmp",  
    "readonly":False
        })
    try:
        m.get_all_mail()
    except Exception as e:
        tomb.tombstone()
        tomb.tombstone(uu.type_and_text(e))

    try:
        m.get_some_mail('from=gflanagi')
    except Exception as e:
        tomb.tombstone()
        tomb.tombstone(uu.type_and_text(e))

    try:
        m.get_some_mail('from=kchapman')
    except Exception as e:
        tomb.tombstone()
        tomb.tombstone(uu.type_and_text(e))

    try:
        m.get_some_mail('foo=bar')
    except Exception as e:
        tomb.tombstone()
        tomb.tombstone(uu.type_and_text(e))

else:
    # print(str(os.path.abspath(__file__)) + " compiled.")
    print("*", end="")


