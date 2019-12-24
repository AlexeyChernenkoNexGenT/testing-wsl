import moment from 'moment';
import { DynamoDBStreams } from 'aws-sdk';
import { STATUS_BAD_REQUEST, STATUS_INTERNAL_SERVER_ERROR } from '@nexgent/common/consts';
import BaseHandler from '../BaseHandler';
import ConsoleLogger from '../../logger/ConsoleLogger';
import Redis from '../../redis/Handler';
import MySQL from '../../mysql/Handler';
import { error, success } from '../response';
import EmailTrackingRepository from '../../mysql/repositories/EmailTrackingRepository';
import { EmailTrackingRecord } from '../../mysql/models/email/EmailTrackingRecord';

type DynamoDBStringValue = {
  S: string;
};

type DynamoDBNumberValue = {
  N: string;
};

type DynamoDBEmailTrackingRecord = {
  ip: DynamoDBStringValue;
  event: DynamoDBStringValue;
  sg_event_id: DynamoDBStringValue;
  'smtp-id': DynamoDBStringValue;
  sg_message_id: DynamoDBStringValue;
  sg_template_id: DynamoDBStringValue;
  sg_template_name: DynamoDBStringValue;
  url?: DynamoDBStringValue;
  templateName: DynamoDBStringValue;
  moduleId: DynamoDBNumberValue;
  userId: DynamoDBNumberValue;
  timestamp: DynamoDBNumberValue;
};

export default class EmailTracking extends BaseHandler {
  mysql: MySQL;
  emailTrackingRepository: EmailTrackingRepository;

  constructor(logger: ConsoleLogger, redis: Redis, mysql: MySQL) {
    super(logger, redis);
    this.mysql = mysql;
    this.emailTrackingRepository = new EmailTrackingRepository(this.mysql);
  }

  convertToDbRecord(item: DynamoDBEmailTrackingRecord): EmailTrackingRecord {
    return {
      module_id: Number(item.moduleId.N),
      user_id: Number(item.userId.N),
      template_name: item.templateName.S,
      email_engagement: item.event.S,
      url: item.url ? item.url.S : null,
      timestamp: moment(item.timestamp.N, 'X').toDate(),
    };
  }

  isValidRecord(item: DynamoDBStreams.Types.Record): boolean {
    return !!(
      item.eventName === 'INSERT' &&
      item.dynamodb &&
      item.dynamodb.NewImage &&
      item.dynamodb.NewImage.templateName &&
      item.dynamodb.NewImage.userId &&
      item.dynamodb.NewImage.moduleId
    );
  }

  async handleRequest(event: DynamoDBStreams.Types.GetRecordsOutput): Promise<object> {
    if (!event || !event.Records) {
      return error(STATUS_BAD_REQUEST, 'Invalid request payload');
    }
    const dbRecords = event.Records.filter(this.isValidRecord).map(p =>
      // @ts-ignore
      this.convertToDbRecord(p.dynamodb.NewImage),
    );

    if (dbRecords.length) {
      try {
        await this.mysql.connect(true);
        await this.mysql.beginTransaction();
        await this.emailTrackingRepository.addRecords(dbRecords);
        await this.mysql.commit();
      } catch (err) {
        this.logger.errorWithStack('EmailTracking failed', err);
        await this.mysql.rollback();
        return error(STATUS_INTERNAL_SERVER_ERROR, 'Could not process request');
      } finally {
        await this.mysql.close();
      }
    }
    return success();
  }
}
